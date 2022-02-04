package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


object GmvApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.消费Kafka中的数据
    val kafkaDStream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)
    kafkaDStream.print()
    //4.将Json字符串转为样例类并补全字段
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.mapPartitions(
      partition => {
        partition.map(record => {
          //将Json字符串转化为样例类
          val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

          //补全时间字段
          orderInfo.create_date = orderInfo.create_time.split(" ")(0);
          orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)

          //对手机号进行脱敏
          orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 3) + "********"

          orderInfo
        })
      })
    orderInfoDStream.print()

    //8.将明细写入Phoenix
    orderInfoDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix(
        "GMALL210826_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    ssc.start()//启动receiverStream
    ssc.awaitTermination()//阻塞主方法
  }
}
