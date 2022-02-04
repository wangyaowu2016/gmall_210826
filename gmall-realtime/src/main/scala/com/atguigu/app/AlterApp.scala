package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlterApp {
  def main(args: Array[String]): Unit = {
    //TODO 需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且过程中没有浏览商品
    //1.创建SparkConf
    val conf : SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlterApp")

    //2.创建StreamingContext
    val ssc : StreamingContext = new StreamingContext(conf, Seconds(5))

    //3.消费kafka中用户行为的事件日志
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT,ssc)

    //4.将读取过来的Json字符串转换为样例类
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val eventLogDStream = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

        val times: String = sdf.format(new Date(eventLog.ts))
        eventLog.logDate = times.split(" ")(0)
        eventLog.logHour = times.split(" ")(1)

        (eventLog.mid,eventLog)
      })
    })

    //5.开启一个5min的窗口
    val WindowDstream = eventLogDStream.window(Seconds(5))

    //6.对相同mid的数据进行聚合
    val midToIterLogDStream: DStream[(String, Iterable[EventLog])] = WindowDstream.groupByKey()

    /*            for (elem <- inter) {
      elem
    }*/// 两种方法都可以

    //7.根据用户行为进行过滤去重(生成疑似预警日志)
    val boolToCouponAlertInfoDStream: DStream[(Boolean, CouponAlertInfo)] = midToIterLogDStream.mapPartitions(
      partition => {
        partition.map {
          case (mid, inter) => {

            //创建set集合用来存放优惠券所涉及的用户id

            val uids = new util.HashSet[String]()

            //创建set集合用来存放优惠券所涉及的商品id
            val itemIds = new util.HashSet[String]()

            //创建List集合用来存放用户所涉及的事件
            val events = new util.ArrayList[String]()

            //定义一个标志位，一旦有浏览商品行为，则置为false
            var bool: Boolean = true

            breakable {
              inter.foreach(log => {
                events.add(log.evid)
                //判断是否浏览商品
                if ("clickItem".equals(log.evid)) {
                  //跳出循环
                  bool = false
                  break()
                } else if ("coupon".equals(log.evid)) {
                  //没有浏览商品但是领取优惠券
                  uids.add(log.uid)
                  itemIds.add(log.itemid)
                }

              })
            }
            (uids.size() >= 3 && bool, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
          }
        }
      })

    //8.生成预警日志(对疑似预警日志进行过滤）
    val couponAlertInfoDStream: DStream[CouponAlertInfo] = boolToCouponAlertInfoDStream.filter(_._1).map(_._2)

    //9.保存至ES
    couponAlertInfoDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(
        partition=>{
          val list: List[(String, CouponAlertInfo)] = partition.toList.map(log => {
            (log.mid + log.ts / 1000 / 60, log)
          })
          MyEsUtil.insertBulk(GmallConstants.ES_ALERT_INDEXNAME + "0826", list)
//          MyEsUtil.insertBulk(GmallConstants.ES_ALERT_INDEXNAME +new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis())), list)
        }
      )
    })

    //10.开启并阻塞
    ssc.start()
    ssc.awaitTermination()

  }
}
