package com.atguigu.handler

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util.Date

object DauHandler {
  /**
   * 批次内去重
   * @param filterByRedisDStream
   * @return
   */
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    //1.将数据转为K，V类型
    val midWithLogDateToStartLogDStream = filterByRedisDStream.map(
      startLog => {
        (startLog.mid + startLog.logDate, startLog)
      })

    //2.按当天的mid的数据聚合到一块
    val midWithLogDateToIterStartLogDStream = midWithLogDateToStartLogDStream.groupByKey()
    //3.对迭代器中的数据做排序
    val midWithLogDateToListStartLogDStream = midWithLogDateToIterStartLogDStream.mapValues(
      Iter => {
        Iter.toList.sortWith(_.ts < _.ts).take(1)
      })

    //4.取出value并打散
    val value = midWithLogDateToListStartLogDStream.flatMap(_._2)
    value

  }

  /**
   * 批次间去重
   * @param startUpLogDStream
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog],sc:SparkContext) = {
/*    val value = startUpLogDStream.filter(
      log => {
        //1.获取redis连接
        println("创建redis连接")
        val jedis = new Jedis("hadoop102", 6379)
        //2.获取redis中的数据
        val rediskey = "DAU" + log.logDate
        //        val mids = jedis.smembers(rediskey)
        //        //3.判断当前id是否包含在redis中
        //        val bool = mids.contains(log.mid)

        //3.直接利用redis中的set类型的方法判断是否存在
        val bool = jedis.sismember(rediskey, log.mid)

        jedis.close()
        !bool
      })
    value*/
    //方案二 在每个分区下获取连接
/*    startUpLogDStream.mapPartitions(
      partition=>{
        //创建redis连接
        println("创建redis连接")
        val jedis = new Jedis("hadoop102", 6379)
        val logs = partition.filter(
          log => {
            val rediskey = "DAU" + log.logDate
            //3.直接利用redis中的set类型的方法判断是否存在
            val bool = jedis.sismember(rediskey, log.mid)
            !bool
          }
        )
        jedis.close()
        logs
      }
    )*/
    //方案三 在每个批次下获取一次连接
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val value1 = startUpLogDStream.transform(//可以不返回
      rdd => {
        //创建redis连接
        val jedis = new Jedis("hadoop102", 6379)

        //在Dirver中获取redis中的数据
        val rediskey = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))
        val mids = jedis.smembers(rediskey)

        //将redis中查询出来的数据广播到executor端
        val midBc = sc.broadcast(mids)

        val value = rdd.filter(
          log => {
            val bool = midBc.value.contains(log.mid)
            !bool
          })
        jedis.close()
        value
      })
    value1
  }

  /**
   * 将mid保存到redis
   *
   * @param startUpLogDStream
   * @return
   */
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          partition => {
            //创建redis连接
            val jedis = new Jedis("hadoop102", 6379)
            partition.foreach(
              startUpLog => {
                //将数据写入redis中
                val rediskey = "DAU:" + startUpLog.logDate
                jedis.sadd(rediskey,startUpLog.mid)
              })
            //在分区下关闭连接
            jedis.close()
          })
      })
  }
}
