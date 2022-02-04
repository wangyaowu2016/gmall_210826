package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

import java.text.SimpleDateFormat
import java.util.Date

object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.利用kafka工具类，获取kafka中的数据
    val kafkaDStream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //4.获取kafka中具体的数据
    kafkaDStream.foreachRDD(
      rdd => {
        rdd.foreach(record => {
          println(record.value())
        }
        )
      }
    )

    //4.将Json字符串转为样例类并补全字段
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")//运行在Driver端，因为支持序列化，所以可以直接在executor端使用
    val startUpLogDStream = kafkaDStream.mapPartitions(
      partition => {
        partition.map(record => {
          //将Json字符串转化为样例类
          val startUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

          //补全时间字段
          val times = sdf.format(new Date(startUpLog.ts))
          startUpLog.logDate = times.split(" ")(0)
          startUpLog.logHour = times.split(" ")(1)
          startUpLog
        })
      })
    //SparkStreaming 默认缓存级别 , ser带序列化，效率更高，占用内存更少
//    def persist(): DStream[T] = persist(StorageLevel.MEMORY_ONLY_SER)

    startUpLogDStream.cache()
    //    打印原始数据的个数
    startUpLogDStream.count().print()

    //5.批次间去重
    val filterByRedisDStream = DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)
    filterByRedisDStream.cache()
//    打印经过批次间去重后的数据的个数
    filterByRedisDStream.count().print()

    //6.批次内去重
    val filterByGroupDStream = DauHandler.filterByGroup(filterByRedisDStream)
    filterByGroupDStream.cache()
    filterByGroupDStream.count().print()

    //7.将去重后的mid写入redis
    DauHandler.saveMidToRedis(filterByGroupDStream)

    //8.将去重后的明细写入Hbase
    filterByGroupDStream.foreachRDD(
      rdd => {
        rdd.saveToPhoenix(
          "GMALL210826_DAU",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
          HBaseConfiguration.create,
          Some("hadoop102,hadoop103,hadoop104:2181"))
      }
    )

    ssc.start()//启动receiverStream
    ssc.awaitTermination()//阻塞主方法
  }
}
