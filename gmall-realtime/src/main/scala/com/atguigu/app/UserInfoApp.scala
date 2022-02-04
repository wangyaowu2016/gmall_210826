package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //TODO 1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserInfoApp")

    //TODO 2.利用SparkConf创建SparkStreamingContext对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //3.消费Kafka中的数据
    val userInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)


    //4.将数据转为样例类
    val userInfoDStream: DStream[UserInfo] = userInfoKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
        userInfo
      })
    })
    userInfoDStream.print()

    userInfoKafkaDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //创建redis连接
        val jedis = new Jedis("hadoop102",6379)
        partition.foreach(record=>{
          //将userInfo数据写入Redis中
          val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
          val userInfoRedisKey: String = "UserInfo:" + userInfo.id
          jedis.set(userInfoRedisKey,record.value())
        })
        jedis.close()
      })
    })

    //TODO 3.启动任务并阻塞线程
    ssc.start()
    ssc.awaitTermination()
  }
}
