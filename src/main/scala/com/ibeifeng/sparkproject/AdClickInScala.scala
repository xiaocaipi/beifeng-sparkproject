package com.ibeifeng.sparkproject

import java.util
import java.util.{Arrays, Date}

import com.ibeifeng.sparkproject.conf.ConfigurationManager
import com.ibeifeng.sparkproject.constant.Constants
import com.ibeifeng.sparkproject.dao._
import com.ibeifeng.sparkproject.dao.factory.DAOFactory
import com.ibeifeng.sparkproject.domain._
import com.ibeifeng.sparkproject.jdbc.JDBCHelper
import com.ibeifeng.sparkproject.util.{DateUtils, SparkUtils}
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, RowFactory, Row}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkContext, KafkaManager, SparkConf}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by caidanfeng733 on 8/17/17.
  */
object AdClickInScala {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.ERROR)

    val sparkconf = new SparkConf().setAppName("AdClickRealTimeStatSpark").setMaster("local[4]")
    //    SparkUtils.setMaster(sparkconf)
    val hdfsPath: String = ConfigurationManager.getProperty(Constants.HDFS_STREAMING_CHECKPOINT_PATH)

    val ssc = new StreamingContext(sparkconf, Seconds(3))
        ssc.checkpoint(hdfsPath)
    ssc.sparkContext.setLogLevel("ERROR")

    val kafkaTopics: String = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS)
    val topicsSet = kafkaTopics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST),
      "group.id" -> "testkafka_group",
      "auto.offset.reset" -> "largest"
    )
    val km = new KafkaManager(kafkaParams)
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    //        val topicMap = kafkaTopics.split(",").map((_, 1.toInt)).toMap
    //        val messages = KafkaUtils.createStream(ssc, "cdh1", "testkafka_group", topicMap)

    //    messages.foreachRDD { rdds =>
    //      val returnrdd = rdds.map(_._2)
    //      val log =returnrdd.collect();
    //      println(rdds + "----"+log)
    //      val offsetsList = rdds.asInstanceOf[HasOffsetRanges].offsetRanges
    //      print(offsetsList)
    //    }

    val adBlacklistDAO: IAdBlacklistDAO = DAOFactory.getAdBlacklistDAO
    val adBlacklists: java.util.List[AdBlacklist] = adBlacklistDAO.findAll
    //    val adBlacklists: java.util.List[AdBlacklist] = new java.util.ArrayList[AdBlacklist]()
    var adBlackSeq = scala.collection.mutable.ListBuffer[AdBlacklist]()
    import scala.collection.JavaConversions._
    for (adBlack <- adBlacklists) {
      adBlackSeq += adBlack
    }

    val sc = ssc.sparkContext
    val blacklistRDDD: RDD[AdBlacklist] = sc.parallelize(adBlackSeq);

    val filteredAdRealTimeLogDStream = filterByBlacklist(messages, blacklistRDDD)
    generateDynamicBlacklist(filteredAdRealTimeLogDStream, km)
    val adRealTimeStatDStream:DStream[(String,java.lang.Long)]=calculateRealTimeStat(filteredAdRealTimeLogDStream)
    calculateProvinceTop3Ad(adRealTimeStatDStream)
    calculateAdClickCountByWindow(messages)


    messages.foreachRDD { rdd =>
      km.updateZKOffsets(rdd)
    }



    ssc.start()
    ssc.awaitTermination()


  }

  def filterByBlacklist(adRealTimeLogDStream: DStream[(String, String)], blacklistRDD: RDD[AdBlacklist]): DStream[(String, String)] = {
    val blackTupleRdd = blacklistRDD.map(adBlack =>
      (adBlack.getUserid, true)
    )
    adRealTimeLogDStream.transform { rdd =>
      val mappedRDD = rdd.map { tuple =>
        val log: String = tuple._2
        val logSplited: Array[String] = log.split(" ")
        val userid: java.lang.Long = java.lang.Long.valueOf(logSplited(3))
        (userid, tuple)
      }
      val blackTupleRdd: RDD[(java.lang.Long, Boolean)] = blacklistRDD.map(x => (x.getUserid, true))
      val joinedRDD = mappedRDD.leftOuterJoin(blackTupleRdd)
      val filteredRDD = joinedRDD.filter { tuple =>
        var returnValue = true
        val option: Option[Boolean] = tuple._2._2
        if (option.getOrElse(false)) {
          returnValue = false
        }
        returnValue
      }
      val returnRDD = filteredRDD.map(x => x._2._1)
      returnRDD
    }

  }

  def generateDynamicBlacklist(filteredAdRealTimeLogDStream: DStream[(String, String)], km: KafkaManager): Unit = {
    val dailyUserAdClickDStream: DStream[(String, java.lang.Long)] = filteredAdRealTimeLogDStream.map { tuple =>
      val log: String = tuple._2
      val logSplited: Array[String] = log.split(" ")
      val timestamp: String = logSplited(0)
      val date: Date = new Date(java.lang.Long.valueOf(timestamp))
      val datekey: String = DateUtils.formatDateKey(date)

      val userid: Long = java.lang.Long.valueOf(logSplited(3))
      val adid: Long = java.lang.Long.valueOf(logSplited(4))

      val key: String = datekey + "_" + userid + "_" + adid
      (key, 1l)
    }
    val dailyUserAdClickCountDStream: DStream[(String, java.lang.Long)] = dailyUserAdClickDStream.reduceByKey((x1, x2) => x1 + x2)

    dailyUserAdClickCountDStream.foreachRDD { rdd =>
      rdd.foreachPartition { iterator =>
        val adUserClickCounts: java.util.List[AdUserClickCount] = new java.util.ArrayList[AdUserClickCount]
        iterator.foreach { tuple =>
          val keySplited: Array[String] = tuple._1.split("_")
          val date: String = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))
          val userid: Long = java.lang.Long.valueOf(keySplited(1))
          val adid: Long = java.lang.Long.valueOf(keySplited(2))
          val clickCount: Long = tuple._2
          val adUserClickCount: AdUserClickCount = new AdUserClickCount
          adUserClickCount.setDate(date)
          adUserClickCount.setUserid(userid)
          adUserClickCount.setAdid(adid)
          adUserClickCount.setClickCount(clickCount)

          adUserClickCounts.add(adUserClickCount)
        }
        val adUserClickCountDAO: IAdUserClickCountDAO = DAOFactory.getAdUserClickCountDAO
        adUserClickCountDAO.updateBatch(adUserClickCounts)
      }
    }

    val blacklistDStream :DStream[(String,java.lang.Long)]=dailyUserAdClickCountDStream.filter { tuple =>
      var returnValue: Boolean = true;

      val key: String = tuple._1
      val keySplited: Array[String] = key.split("_")

      val date: String = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))
      val userid: Long = java.lang.Long.valueOf(keySplited(1))
      val adid: Long = java.lang.Long.valueOf(keySplited(2))

      val adUserClickCountDAO: IAdUserClickCountDAO = DAOFactory.getAdUserClickCountDAO
      val clickCount: Int = adUserClickCountDAO.findClickCountByMultiKey(date, userid, adid)
      if (clickCount >= 100) {
        returnValue= true
      }else{
        returnValue = false
      }

      returnValue

    }

    val blacklistUseridDStream:DStream[java.lang.Long]=blacklistDStream.map{tuple=>
      val key: String = tuple._1
      val keySplited: Array[String] = key.split("_")
      val userid: java.lang.Long = java.lang.Long.valueOf(keySplited(1))
      userid
    }

   val distinctBlacklistUseridDStream:DStream[java.lang.Long]= blacklistUseridDStream.transform(rdd=>rdd.distinct())

    distinctBlacklistUseridDStream.foreachRDD{rdd=>
      rdd.foreachPartition{iterator=>
        val adBlacklists: java.util.List[AdBlacklist] = new java.util.ArrayList[AdBlacklist]
        iterator.foreach{userId=>
          val adBlacklist: AdBlacklist = new AdBlacklist
          adBlacklist.setUserid(userId)

          adBlacklists.add(adBlacklist)
        }
        val adBlacklistDAO: IAdBlacklistDAO = DAOFactory.getAdBlacklistDAO
        adBlacklistDAO.insertBatch(adBlacklists)
      }
    }



    //    dailyUserAdClickCountDStream.print()
  }

  def calculateRealTimeStat  (filteredAdRealTimeLogDStream: DStream[(String, String)]) :DStream[(String,java.lang.Long)]={
     val mappedDStream:DStream[(String,java.lang.Long)] = filteredAdRealTimeLogDStream.map{tuple=>

       val log: String = tuple._2
       val logSplited: Array[String] = log.split(" ")

       val timestamp: String = logSplited(0)
       val date: Date = new Date(java.lang.Long.valueOf(timestamp))
       val datekey: String = DateUtils.formatDateKey(date)

       val province: String = logSplited(1)
       val city: String = logSplited(2)
       val adid: Long = java.lang.Long.valueOf(logSplited(4))

       val key: String = datekey + "_" + province + "_" + city + "_" + adid
       (key,1l)
     }

    val updateFunc = (values: Seq[java.lang.Long], previouState: Option[java.lang.Long]) =>{
       var previous:java.lang.Long = previouState.getOrElse(0l)
      for(value <- values){
        previous = previous +value
      }
      Some(previous)
    }
    val aggregatedDStream:DStream[(String,java.lang.Long)]=mappedDStream.updateStateByKey(updateFunc)

    aggregatedDStream.foreachRDD{rdd=>
      rdd.foreachPartition{iterator=>
        val adStats: java.util.List[AdStat] = new java.util.ArrayList[AdStat]
        iterator.foreach{tuple=>
          val keySplited: Array[String] = tuple._1.split("_")
          val date: String = keySplited(0)
          val province: String = keySplited(1)
          val city: String = keySplited(2)
          val adid: Long = java.lang.Long.valueOf(keySplited(3))

          val clickCount: Long = tuple._2

          val adStat: AdStat = new AdStat
          adStat.setDate(date)
          adStat.setProvince(province)
          adStat.setCity(city)
          adStat.setAdid(adid)
          adStat.setClickCount(clickCount)

          adStats.add(adStat)
        }
        val adStatDAO: IAdStatDAO = DAOFactory.getAdStatDAO
        adStatDAO.updateBatch(adStats)
      }
    }


    aggregatedDStream
  }


  def calculateProvinceTop3Ad (adRealTimeStatDStream:DStream[(String,java.lang.Long)]):Unit={


    val returnDstream:DStream[Row]=adRealTimeStatDStream.transform{rdd=>

      val mappedRDD:RDD[(String,java.lang.Long)]= rdd.map{tuple=>
        val keySplited: Array[String] = tuple._1.split("_")
        val date: String = keySplited(0)
        val province: String = keySplited(1)
        val adid: Long = java.lang.Long.valueOf(keySplited(3))
        val clickCount: java.lang.Long = tuple._2

        val key: String = date + "_" + province + "_" + adid
        (key, clickCount)
      }

      val dailyAdClickCountByProvinceRDD:RDD[(String,java.lang.Long)]  =mappedRDD.reduceByKey((v1,v2) =>v1+v2)

      val rowRdd:RDD[Row]= dailyAdClickCountByProvinceRDD.map{tuple=>
        val keySplited: Array[String] = tuple._1.split("_")
        val datekey: String = keySplited(0)
        val province: String = keySplited(1)
        val adid: java.lang.Long = java.lang.Long.valueOf(keySplited(2))
        val clickCount: java.lang.Long = tuple._2

        val date: String = DateUtils.formatDate(DateUtils.parseDateKey(datekey))

        val row :Row = RowFactory.create(date, province, adid, clickCount)
        row
      }

      val schema: StructType = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("date", DataTypes.StringType, true),
                                                                        DataTypes.createStructField("province", DataTypes.StringType, true),
                                                                        DataTypes.createStructField("ad_id", DataTypes.LongType, true),
                                                                        DataTypes.createStructField("click_count", DataTypes.LongType, true)))

      val sqlContext: SQLContext = SQLContextSingleton.getInstance(rdd.context)

      val dailyAdClickCountByProvinceDF: DataFrame = sqlContext.createDataFrame(rowRdd,schema)

      dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov")

      val provinceTop3AdDF: DataFrame = sqlContext.sql("SELECT  date, province, ad_id, click_count  FROM (  SELECT date, province, ad_id, click_count, ROW_NUMBER() OVER (PARTITION BY province ORDER BY click_count DESC) rank  FROM tmp_daily_ad_click_count_by_prov ) t WHERE rank>=3")

      provinceTop3AdDF.rdd
    }


    returnDstream.foreachRDD{rdd=>
        rdd.foreachPartition{iterator=>
          val adProvinceTop3s: java.util.List[AdProvinceTop3] = new java.util.ArrayList[AdProvinceTop3]
            iterator.foreach{row=>
              val date: String = row.getString(0)
              val province: String = row.getString(1)
              val adid: Long = row.getLong(2)
              val clickCount: Long = row.getLong(3)

              val adProvinceTop3: AdProvinceTop3 = new AdProvinceTop3
              adProvinceTop3.setDate(date)
              adProvinceTop3.setProvince(province)
              adProvinceTop3.setAdid(adid)
              adProvinceTop3.setClickCount(clickCount)

              adProvinceTop3s.add(adProvinceTop3)
            }

          val adProvinceTop3DAO: IAdProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO
          adProvinceTop3DAO.updateBatch(adProvinceTop3s)
        }
    }





  }

  def calculateAdClickCountByWindow (adRealTimeLogDStream:DStream[(String,String)]) :Unit={

    val pairDStream:DStream[(String,java.lang.Long)] =  adRealTimeLogDStream.map{tuple=>
      val logSplited: Array[String] = tuple._2.split(" ")
      val timeMinute: String = DateUtils.formatTimeSecond(new Date(java.lang.Long.valueOf(logSplited(0))))
      val adid: java.lang.Long = java.lang.Long.valueOf(logSplited(4))
      val value:java.lang.Long=1l
      (timeMinute + "_" + adid, value)
    }

//    pairDStream.reduceByKeyAndWindow((a1:Long,a2:Long) => {
//      a1 +a2
//    }, Seconds(120), Seconds(3))

    val aggrRDD:DStream[(String,java.lang.Long)] =pairDStream.reduceByKeyAndWindow((a1:java.lang.Long,a2:java.lang.Long) =>addFun(a1,a2), Seconds(60), Seconds(3))

    aggrRDD.print()

//    aggrRDD.foreachRDD{rdd=>
//      rdd.foreachPartition{iterator=>
//        val adClickTrends: java.util.List[AdClickTrend] = new java.util.ArrayList[AdClickTrend]
//        iterator.foreach{tuple=>
//          val keySplited: Array[String] = tuple._1.split("_")
//          val dateMinute: String = keySplited(0)
//          val adid: java.lang.Long = java.lang.Long.valueOf(keySplited(1))
//          val clickCount: java.lang.Long = tuple._2
//
//          val date: String = DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0, 8)))
//          val hour: String = dateMinute.substring(8, 10)
//          val minute: String = dateMinute.substring(10)
//
//          val adClickTrend: AdClickTrend = new AdClickTrend
//          adClickTrend.setDate(date)
//          adClickTrend.setHour(hour)
//          adClickTrend.setMinute(minute)
//          adClickTrend.setAdid(adid)
//          adClickTrend.setClickCount(clickCount)
//
//          adClickTrends.add(adClickTrend)
//        }
//
//        val adClickTrendDAO: IAdClickTrendDAO = DAOFactory.getAdClickTrendDAO
//        adClickTrendDAO.updateBatch(adClickTrends)
//
//      }
//    }

  }

  def addFun(a1:java.lang.Long,a2:java.lang.Long):java.lang.Long={
    a1+a2
  }

  object SQLContextSingleton {

    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new HiveContext(sparkContext)
      }
      instance
    }
  }

}
