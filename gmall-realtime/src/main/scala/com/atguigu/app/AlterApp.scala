package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{EventLog, StartUpLog}
import com.atguigu.constants.GmallConstants
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import com.atguigu.utils.MyKafkaUtil
import org.apache.spark.streaming.dstream.DStream

import java.text.SimpleDateFormat
import java.util.Date
import scala.util.control.Breaks.{break, breakable}

object AlterApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf : SparkConf = new SparkConf().setAppName("AlterApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc : StreamingContext = new StreamingContext(conf, Seconds(5))

    //3.消费Kafka中用户行为的事件日志
    val kafkaDStream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //4.将读取过来的Json字符串转换为样例类
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val EventLogDStream = kafkaDStream.mapPartitions(
      partition => {
        partition.map(record => {

          //将Json字符串转化为样例类
          val EventLog = JSON.parseObject(record.value(), classOf[EventLog])
          val times = sdf.format(new Date(EventLog.ts))

          EventLog.logDate = times.split(" ")(0)
          EventLog.logHour = times.split(" ")(1)
          (EventLog.mid,EventLog)
        })
      }
    )
    EventLogDStream
    //5.开启一个5min的窗口
    val WindowDstream = EventLogDStream.window(Minutes(5))
    //6.对相同mid的数据进行聚合
    val midToInterLogDStream: DStream[(String, Iterable[EventLog])] = EventLogDStream.groupByKey()

    //7.根据用户行为进行过滤去重
    midToInterLogDStream.mapPartitions(
      partition=>{
        partition.map{
          case (mid,inter)=> {
            /*            for (elem <- inter) {

            }*/
            //创建set集合用来存放优惠券所涉及的用户id
            new util.HashSet[String]()
            //创建set集合用来存放优惠券所涉及的商品id

            breakable {
              inter.foreach(
                log => {
                  //判断是否浏览商品
                  if ("clickItem".equals(log.evid)) {
                    //跳出循环
                    break()
                  }else if ("coupon".equals(log.evid))
                  {
                    //没有浏览商品但是领取优惠券
                    uids.add(log.uid)
                  }
                }
              )
            }
          }
        }
      }
    )

    //8.生成预警日志

    //9.保存至ES

    //10.开启并阻塞
    ssc.start()
    ssc.awaitTermination()

  }

}
