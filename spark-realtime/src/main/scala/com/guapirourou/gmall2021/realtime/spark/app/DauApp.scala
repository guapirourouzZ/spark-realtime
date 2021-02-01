package com.guapirourou.gmall2021.realtime.spark.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.guapirourou.gmall2021.realtime.spark.util.{MyKafkaUtil, OffsetManagerUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object DauApp {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DAU_APP")

        val ssc = new StreamingContext(sparkConf, Seconds(5))
        val topic = "ODS_BASE_LOG"
        val groupid = "dau_app_group"

        val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupid)

        var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
        //如果能取到偏移量则从偏移量位置取得数据量 否则 从最新的位置取得数据流
        if (offsetMap == null || offsetMap.size < 0) {
            inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupid)
        } else {
            inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupid)
        }


        //3 获取偏移量结束点
        var offsetRanges: Array[OffsetRange] = null //driver
        //从流中顺手牵羊把本批次的偏移量存入全局变量中。
        val inputDstreamWithOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
            //driver//在driver中周期性 执行  可以写在transform中，或者从rdd中提取数据比如偏移量
            val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
            offsetRanges = hasOffsetRanges.offsetRanges
            //            rdd.map{a => //executor
            //
            //            }
            rdd
        }

        //把ts时间戳转换成日期和小时，为后续便于处理
        val jsonStringDstream: DStream[JSONObject] = inputDstreamWithOffsetDstream.map(record => {
            val jsonString: String = record.value()
            val jSONObject: JSONObject = JSON.parseObject(jsonString)
            val ts: lang.Long = jSONObject.getLong("ts")
            val dateHourStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
            val dateHour: Array[String] = dateHourStr.split(" ")
            val date: String = dateHour(0)
            val hour: String = dateHour(1)
            jSONObject.put("dt", date);
            jSONObject.put("hr", hour);
            jSONObject
        }

        )

        //1.筛选出用户最基本的活跃行为（打开第一个页面（page项中，没有last_page_id））
        val firstPageJsonObjDstream: DStream[JSONObject] = jsonStringDstream.filter { jsonObj =>

            val pageJsonObj: JSONObject = jsonObj.getJSONObject("page")
            if (pageJsonObj != null) {
                val lastPageId: String = pageJsonObj.getString("last_page_id")
                if (lastPageId == null || lastPageId.length == 0) {
                    true
                } else {
                    false
                }
            } else {
                false
            }
        }

        firstPageJsonObjDstream.cache()

//        firstPageJsonObjDstream.count().print()

        /*
                //2.去重，以什么字段为准进行去重（用户id，ip，mid），用redis来存储已访问列表 什么数据对象来存储
                val dauDstream: DStream[JSONObject] = firstPageJsonObjDstream.filter { jsonObj =>
                    //提取对象的mid
                    val mid: String = jsonObj.getJSONObject("common").getString("mid")
                    val dt: String = jsonObj.getString("dt")


                    //查询 列表中是否有该mid
                    //设计定义 已访问设备列表
                    //redis
                    // type? string(可以去重setnx exist)(每个 mid-每天 从成为一个key，极端情况下的好处：利用分布式)
                    // √set(把当天已访问存入 一个set key)
                    // list(x不能去重)
                    // zset(x不需要排序)
                    // hash 对set的扩展 (x不需要两个字段)
                    // key? dau:[2021-01-22](filed score?) value? mid    expire超时时间?24小时 读api sadd自带判断是否存在 写api sadd

                    //val jedis = new Jedis("hadoop102", 6379)
                    //使用线程池
                    val jedis: Jedis = RedisUtil.getJedisClient //driver

                    val key = "dau:" + dt
                    val isNew: lang.Long = jedis.sadd(key, mid)
                    jedis.expire(key, 3600 * 24)
                    jedis.close()

                    if (isNew == 1L) {
                        true
                    } else {
                        false
                    }
                    //如果有  过滤掉该对象  如果没有保留，插入到该表中
                }
        */

        //优化过：优化目的 减少创建（获取）连接的次数，做成每批次每分区 执行一次
        val dauDstream: DStream[JSONObject] = firstPageJsonObjDstream.mapPartitions { jsonObjItr =>
            val jedis: Jedis = RedisUtil.getJedisClient //该批次 该分区   执行一次

            val filteredList: ListBuffer[JSONObject] = ListBuffer[JSONObject]()

            for (jsonObj <- jsonObjItr) { //条为单位处理
                //提取对象的mid
                val mid: String = jsonObj.getJSONObject("common").getString("mid")
                val dt: String = jsonObj.getString("dt")

                val key = "dau:" + dt
                val isNew: lang.Long = jedis.sadd(key, mid)
                jedis.expire(key, 3600 * 24)

                //如果有（非新）放弃，（新的）保留
                if (isNew == 1L) {
                    filteredList.append(jsonObj)
                }
            }
            jedis.close()
            filteredList.toIterator
        }


        dauDstream.print()

        dauDstream.foreachRDD {  rdd =>
            rdd.foreachPartition { jsonObjItr =>
                //存储jsonObjItr 整体 executor
                for (jsonObj <- jsonObjItr ) {
                    println(jsonObj)
                }
                //1 executor 每批次 每分区
//                OffsetManagerUtil.saveOffset(topic,groupid, offsetRanges)

            }
            //2 driver 周期性执行 //2 在driver存储driver全局数据，每个批次保存一次偏移量
            OffsetManagerUtil.saveOffset(topic,groupid, offsetRanges)
            println("AAAA")

        }
        //3 driver 执行一次
//        OffsetManagerUtil.saveOffset(topic,groupid, offsetRanges)
        println("BBBB")


        ssc.start()
        ssc.awaitTermination()
    }

}
