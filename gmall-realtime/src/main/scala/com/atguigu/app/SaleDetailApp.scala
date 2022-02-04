package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import java.util
import collection.JavaConverters._

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")

    //2.创建StramingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //3.消费Kafka中的数据
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    val OrderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    //4.将数据转为样例类
    val orderInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        (orderInfo.id, orderInfo)
      })
    })

    val orderDetailDStream = OrderDetailKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.id, orderDetail)
      })
    })
//    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoDStream.join(orderDetailDStream)
    //5.使用外连接连接两条流，为了防止连接不上时丢失数据
    val fulljoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

    //6.通过加缓存的方式解决数据因网络延迟所导致的数据丢失问题
    val noUserSaleDetailDStream: DStream[SaleDetail] = fulljoinDStream.mapPartitions(partition => {
      //样例类转换成为JSON字符串
      implicit val formats = org.json4s.DefaultFormats

      //创建存放saleDetail的集合
      val saleDetails = new util.ArrayList[SaleDetail]()

      //创建Redis连接
      val jedis = new Jedis("hadoop102", 6379)

      partition.foreach { case (orderId, (infoOpt, detailOpt)) =>
        //orderInfo对应的redis的key
        val infoRedisKey: String = "OrderInfo:" + orderId

        //orderDetail对应的redis的key
        val detailRedisKey: String = "OrderDetail:" + orderId

        //a.1 判断orderInfo是否存在
        if (infoOpt.isDefined) {
          //orderInfo存在
          //提取orderInfo数据
          val orderInfo: OrderInfo = infoOpt.get
          //a.2 判断orderDetail是否存在
          if (detailOpt.isDefined) {
            //OrderDetail数据存在
            //取出OrderDetail数据
            val orderDetail: OrderDetail = detailOpt.get
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            //a.3 将关联起来的数据存入结果集合中
            saleDetails.add(saleDetail)
          }
          //b.1 将orderInfo数据转为JSON写入redis中
          //          JSON.toJSONString(orderInfo)//只能将JavaBean转为JSON 不能将样例类转为JSON 否则编译会报错
          //样例类转换成为JSON字符串
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedis.set(infoRedisKey, orderInfoJson)

          //b.2 为了防止数据挤压，为其设置过期时间(最好设置集群延迟的最大时间，询问老员工或自己造数据测)
          jedis.expire(infoRedisKey, 30)

          //c.1 查询OrderDetail缓存中是否有对应的数据
          //判断redis中是否存有orderId对应的OrderDetail数据
          if (jedis.exists(detailRedisKey)) {
            //存在对应的数据
            //获取redis缓存中存放的数据
            val orderDetailJsonSet: util.Set[String] = jedis.smembers(detailRedisKey)
            for (elem <- orderDetailJsonSet.asScala) {
              //将查询出来的OrderDetail对应的JSON转为样例类
              val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
              //写入SaleDetail中
              val detail = new SaleDetail(orderInfo, orderDetail)
              //将关联起来的数据存入结果集合中
              saleDetails.add(detail)
            }
          }
        } else {
          //orderInfo不在
          //c.1 判断orderDetail是否存在
          if (detailOpt.isDefined) {
            //orderDetail存在
            //取出orderDetail存在数据
            val orderDetail: OrderDetail = detailOpt.get

            //c.2 orderInfo缓存中查询是否有能关联上的数据
            if (jedis.exists(infoRedisKey)) {
              //有能关联上的OrderInfo数据
              val orderInfoJson: String = jedis.get(infoRedisKey)
              //将字符串转为样例类
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
              //关联到结果集合
              val detail = new SaleDetail(orderInfo, orderDetail)
              //将关联起来的数据存入结果集合中
              saleDetails.add(detail)
            } else {
              //orderInfo缓存中没有能关联上的数据
              //c.3 将自己(orderDetail)存入缓存中
              //将orderDetail样例类转为JSON字符串
              val orderDetailJSONStr: String = Serialization.write(orderDetail)
              jedis.sadd(detailRedisKey, orderDetailJSONStr)
              //设置过期时间
              jedis.expire(detailRedisKey, 30)
            }
          }
        }
      }
      jedis.close()
      saleDetails.asScala.toIterator
    })

    //7.反查缓存补全用户信息
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(partition => {
      val jedis = new Jedis("hadoop102", 6379)
      val details: Iterator[SaleDetail] = partition.map(saleDetail => {
        //查询redis中用户表的数据
        val userInfoRedisKey: String = "UserInfo:" + saleDetail.user_id
        val userInfoJSONStr: String = jedis.get(userInfoRedisKey)

        //将查询出来的数据转为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJSONStr, classOf[UserInfo])

        //补全用户信息
        saleDetail.mergeUserInfo(userInfo)

        saleDetail
      })
      jedis.close()
      details
    })
    saleDetailDStream.print()
    //8.保存到ES中
    saleDetailDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val list: List[(String, SaleDetail)] = partition.toList.map(saleDetail => {
          (saleDetail.order_detail_id, saleDetail)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_DETAIL_INDEXNAME+"0826",list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
