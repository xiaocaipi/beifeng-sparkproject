package com.ibeifeng.sparkproject



import com.alibaba.fastjson.{JSONObject, JSON}
import com.ibeifeng.sparkproject.spark.session.SessionAggrStatAccumulator
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.{AccumulatorParam, Accumulator, SparkConf, SparkContext}
import com.ibeifeng.sparkproject.constant.Constants
import com.ibeifeng.sparkproject.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{SQLContext, DataFrame, RowFactory, Row}
import java.util.{Date, Arrays, UUID, Random}


import scala.Array

object UserVisitSessionInscala {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf()
      .setAppName(Constants.SPARK_APP_NAME_SESSION)
      				.set("spark.default.parallelism", "1")
      .set("spark.storage.memoryFraction", "0.5")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.shuffle.file.buffer", "64")
      .set("spark.shuffle.memoryFraction", "0.3")
      .set("spark.reducer.maxSizeInFlight", "24")
      .set("spark.shuffle.io.maxRetries", "60")
      .set("spark.shuffle.io.retryWait", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    SparkUtils.setMaster(sparkconf)

    val sc = new SparkContext(sparkconf)

    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)

    mockData(sc, sqlcontext)


    var params: String = "{\"startAge\":[\"10\"],\"endAge\":[\"50\"],\"startDate\":[\"2017-08-25\"],\"endDate\":[\"2017-08-28\"]}"
    val taskParam = JSON.parseObject(params)
    val actionRDD = getActionRDDByDateRange(sqlcontext, taskParam)
    val sessionid2actionRDD = getSessionid2ActionRDD(actionRDD)

    val sessionid2AggrInfoRDD = aggregateBySession(sc, sqlcontext, sessionid2actionRDD)
    val aa = new SessionAggrStatAccumulator
    val sessionAggrStatAccumulator: Accumulator[String] = sc.accumulator("")(MySessionAggrStatAccumulator)
    val filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionid2AggrInfoRDD,taskParam,sessionAggrStatAccumulator)
    //过滤后的 action
    val sessionid2detailRDD=getSessionid2detailRDD(filteredSessionid2AggrInfoRDD,sessionid2actionRDD)
    val count = sessionid2detailRDD.count()
    println(count)

//    randomExtractSession(sc,2l,filteredSessionid2AggrInfoRDD,sessionid2detailRDD)

//    print("qianmian "+sessionid2actionRDD.count())
//    print("houmian "+sessionid2detailRDD.count())

//    myPrint(filteredSessionid2AggrInfoRDD,5)
//    println("------------")
//    myPrint(sessionid2actionRDD,5)


    //    val sql = "select * from  user_visit_action limit 5"
    //    sqlcontext.sql(sql).collect().foreach(println)


  }

  def mockData(sc: SparkContext, sqlcontext: org.apache.spark.sql.SQLContext): Unit = {

    var action_rows = scala.collection.mutable.ListBuffer[Row]()
    var product_rows = scala.collection.mutable.ListBuffer[Row]()
    var user_rows = scala.collection.mutable.ListBuffer[Row]()
    val searchKeywords = collection.mutable.ArrayBuffer("火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
      "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
    val date = DateUtils.getTodayDate();

    val actions = collection.mutable.ArrayBuffer("search", "click", "order", "pay")
    val random = new Random();

    for (i <- 0 to 100 - 1) {
      var userid: java.lang.Long = random.nextInt(100).toLong;

      for (j <- 0 to 10 - 1) {
        val sessionid: String = UUID.randomUUID.toString.replace("-", "")
        val baseActionTime: String = date + " " + random.nextInt(23)
        var clickCategoryId: java.lang.Long = null

        for (k <- 0 to random.nextInt(100) - 1) {
          val pageid: java.lang.Long = random.nextInt(10).toLong
          val actionTime: String = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))
          var clickProductId: java.lang.Long = null
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null
          var searchKeyword: String = null

          val action: String = actions(random.nextInt(4))
          if ("search" == action) {
            searchKeyword = searchKeywords(random.nextInt(10))
          }
          else if ("click" == action) {
            if (clickCategoryId == null) {
              clickCategoryId = java.lang.Long.valueOf(String.valueOf(random.nextInt(100)))
            }
            clickProductId = java.lang.Long.valueOf(String.valueOf(random.nextInt(100)))
          }
          else if ("order" == action) {
            orderCategoryIds = String.valueOf(random.nextInt(100))
            orderProductIds = String.valueOf(random.nextInt(100))
          }
          else if ("pay" == action) {
            payCategoryIds = String.valueOf(random.nextInt(100))
            payProductIds = String.valueOf(random.nextInt(100))
          }
          val row: Row = RowFactory.create(date, userid
            , sessionid, pageid, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, java.lang.Long.valueOf(String.valueOf(random.nextInt(10))))
          action_rows += (row)
        }


      }

    }

    var actionRowsRDD = sc.parallelize(action_rows)

    val schema: StructType = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("date", DataTypes.StringType, true), DataTypes.createStructField("user_id", DataTypes.LongType, true), DataTypes.createStructField("session_id", DataTypes.StringType, true), DataTypes.createStructField("page_id", DataTypes.LongType, true), DataTypes.createStructField("action_time", DataTypes.StringType, true), DataTypes.createStructField("search_keyword", DataTypes.StringType, true), DataTypes.createStructField("click_category_id", DataTypes.LongType, true), DataTypes.createStructField("click_product_id", DataTypes.LongType, true), DataTypes.createStructField("order_category_ids", DataTypes.StringType, true), DataTypes.createStructField("order_product_ids", DataTypes.StringType, true), DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true), DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true), DataTypes.createStructField("city_id", DataTypes.LongType, true)))

    val df: DataFrame = sqlcontext.createDataFrame(actionRowsRDD, schema)


    df.registerTempTable("user_visit_action")


    val sexes: Array[String] = Array[String]("male", "female")

    for (i <- 0 to 100 - 1) {
      val userid: java.lang.Long = i.toLong
      val username: String = "user" + i
      val name: String = "name" + i
      val age: java.lang.Integer = random.nextInt(60)
      val professional: String = "professional" + random.nextInt(100)
      val city: String = "city" + random.nextInt(100)
      val sex: String = sexes(random.nextInt(2))

      val row: Row = RowFactory.create(userid, username, name, age, professional, city, sex)
      user_rows += (row)
    }

    var userRowsRDD = sc.parallelize(user_rows)

    val schema2: StructType = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("user_id", DataTypes.LongType, true), DataTypes.createStructField("username", DataTypes.StringType, true), DataTypes.createStructField("name", DataTypes.StringType, true), DataTypes.createStructField("age", DataTypes.IntegerType, true), DataTypes.createStructField("professional", DataTypes.StringType, true), DataTypes.createStructField("city", DataTypes.StringType, true), DataTypes.createStructField("sex", DataTypes.StringType, true)))
    //从rdd 到df
    val df2: DataFrame = sqlcontext.createDataFrame(userRowsRDD, schema2)

    df2.registerTempTable("user_info")


    val productStatus: Array[Int] = Array[Int](0, 1)
    for (i <- 0 to 100 - 1) {
      val productId: java.lang.Long = i.toLong
      val productName: String = "product" + i
      val extendInfo: String = "{\"product_status\": " + productStatus(random.nextInt(2)) + "}"

      val row: Row = RowFactory.create(productId, productName, extendInfo)
      product_rows += (row)
    }

    var productRowsRDD = sc.parallelize(product_rows)

    val schema3: StructType = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("product_id", DataTypes.LongType, true), DataTypes.createStructField("product_name", DataTypes.StringType, true), DataTypes.createStructField("extend_info", DataTypes.StringType, true)))

    val df3: DataFrame = sqlcontext.createDataFrame(productRowsRDD, schema3)
    df3.registerTempTable("product_info")


  }


  def getActionRDDByDateRange(sQLContext: SQLContext, taskParam: JSONObject): RDD[Row] = {
    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    var sql: String = "select * " + "from user_visit_action " + "where date>='" + startDate + "' " + "and date<='" + endDate + "'"
    val actionDf = sQLContext.sql(sql)
    //    myPrint(actionDf.map(_.get(7)),5)
    //    actionDf.map{row =>
    //      Person(row.getString(0)
    //            ,row.getLong(1)
    //            ,row.getString(2)
    //            ,row.getLong(3)
    //            ,row.getString(4)
    //            ,row.getString(5)
    //            ,row.getLong(6)
    //            ,row.getString(7)
    //            ,row.getString(8)
    //            ,row.getString(9)
    //            ,row.getString(10)
    //            ,row.getLong(11))}
    actionDf.rdd

  }

  def getSessionid2ActionRDD(actionRdd: RDD[Row]): RDD[(String, Row)] = {
    //    val returnRdd :RDD[(String,Row)] =actionRdd.mapPartitions{ rows =>
    //      var list = scala.collection.mutable.ListBuffer(String,Row)
    //      for(row <- rows){
    //        list += (row.getString(2),row)
    //      }
    //      return  list
    //    }
    //    val mappari:RDD[(String,Row)] = actionRdd.map(row => (row.getString(2),row))
    val returnRDD: RDD[(String, Row)] = actionRdd.mapPartitions(getSessionRowDef)
    returnRDD
  }

  def getSessionRowDef(iter: Iterator[Row]): Iterator[(String, Row)] = {
    var res = List[(String, Row)]()
    while (iter.hasNext) {
      val cur = iter.next;
      res.::=(cur.getString(2), cur)
    }
    res.iterator
  }

  def aggregateBySession(sc: SparkContext, sqlcontext: SQLContext, sessinoid2actionRDD: RDD[(String, Row)]): RDD[(String, String)] = {
    val sessionid2ActionsRDD: RDD[(String, Iterable[Row])] = sessinoid2actionRDD.groupByKey()
    val userid2PartAggrInfoRDD: RDD[(java.lang.Long, String)] = sessionid2ActionsRDD.map { x =>
      val sessionid = x._1
      val iterator = x._2.iterator
      val searchKeywordsBuffer: StringBuffer = new StringBuffer("")
      val clickCategoryIdsBuffer: StringBuffer = new StringBuffer("")
      var userid: java.lang.Long = null
      var startTime: Date = null
      var endTime: Date = null
      var stepLength: java.lang.Integer = 0

      while (iterator.hasNext) {
        val row: Row = iterator.next
        if (userid == null) {
          userid = row.getLong(1)
        }
        val searchKeyword: String = row.getString(5)
        val clickCategoryId: java.lang.Long = row.getLong(6)

        if (StringUtils.isNotEmpty(searchKeyword)) {
          if (!searchKeywordsBuffer.toString.contains(searchKeyword)) {
            searchKeywordsBuffer.append(searchKeyword + ",")
          }
        }

        if (clickCategoryId != null) {
          if (!clickCategoryIdsBuffer.toString.contains(String.valueOf(clickCategoryId))) {
            clickCategoryIdsBuffer.append(clickCategoryId + ",")
          }
        }

        val actionTime: Date = DateUtils.parseTime(row.getString(4))

        if (startTime == null) {
          startTime = actionTime
        }
        if (endTime == null) {
          endTime = actionTime
        }

        if (actionTime.before(startTime)) {
          startTime = actionTime
        }
        if (actionTime.after(endTime)) {
          endTime = actionTime
        }

        stepLength += 1

      }

      val searchKeywords: String = StringUtils.trimComma(searchKeywordsBuffer.toString)
      val clickCategoryIds: String = StringUtils.trimComma(clickCategoryIdsBuffer.toString)

      val visitLength: java.lang.Long = (endTime.getTime - startTime.getTime) / 1000

      val partAggrInfo: String = Constants.FIELD_SESSION_ID + "=" + sessionid + "|" +
        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
        Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
        Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

      (userid, partAggrInfo)
    }

    val sql: String = "select * from user_info"
    val userInfoRDDTmp:RDD[Row]= sqlcontext.sql(sql).rdd
    val userInfoRDD:RDD[(java.lang.Long,Row)] =userInfoRDDTmp.map(row =>(row.getLong(0),row))
    val sessionid2FullAggrInfoRDD :RDD[(String,String)]=userid2PartAggrInfoRDD.join(userInfoRDD).map{tuple=>
      val partAggrInfo: String = tuple._2._1
      val userInfoRow: Row = tuple._2._2

      val sessionid: String = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
      val age: Int = userInfoRow.getInt(3)
      val professional: String = userInfoRow.getString(4)
      val city: String = userInfoRow.getString(5)
      val sex: String = userInfoRow.getString(6)

      val fullAggrInfo: String = partAggrInfo + "|" +
                                Constants.FIELD_AGE + "=" + age + "|" +
                                Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
                                Constants.FIELD_CITY + "=" + city + "|" +
                                Constants.FIELD_SEX + "=" + sex
      (sessionid,fullAggrInfo)

    }
    sessionid2FullAggrInfoRDD

  }

  def filterSessionAndAggrStat(sessionid2AggrInfoRDD:RDD[(String,String)],taskParam:JSONObject,sessionAggrStatAccumulator:Accumulator[String]):RDD[(String,String)]={
    val startAge: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals: String = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities: String = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex: String = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords: String = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds: String = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var _parameter: String = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") + (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") + (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") + (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") + (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") + (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") + (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if (_parameter.endsWith("\\|")) {
      _parameter = _parameter.substring(0, _parameter.length - 1)
    }

    val  parameter = _parameter;

    val filteredSessionid2AggrInfoRDD:RDD[(String,String)] = sessionid2AggrInfoRDD.filter{tuple=>
      val aggrInfo: String = tuple._2
      var returnvalue =true
      if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
        returnvalue = false
      }
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
        returnvalue =  false
      }
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
        returnvalue =  false
      }
      if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
        returnvalue =  false
      }
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
        returnvalue =  false
      }
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
        returnvalue =  false
      }

      sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)

      val visitLength: java.lang.Long = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH))
      val stepLength: java.lang.Long = java.lang.Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH))

      def calculateVisitLength(visitLength: java.lang.Long):Unit ={
        if (visitLength >= 1 && visitLength <= 3) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
        }
        else if (visitLength >= 4 && visitLength <= 6) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
        }
        else if (visitLength >= 7 && visitLength <= 9) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s)
        }
        else if (visitLength >= 10 && visitLength <= 30) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s)
        }
        else if (visitLength > 30 && visitLength <= 60) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s)
        }
        else if (visitLength > 60 && visitLength <= 180) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m)
        }
        else if (visitLength > 180 && visitLength <= 600) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m)
        }
        else if (visitLength > 600 && visitLength <= 1800) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m)
        }
        else if (visitLength > 1800) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m)
        }
      }


      def calculateStepLength(stepLength: java.lang.Long):Unit ={
        if (stepLength >= 1 && stepLength <= 3) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3)
        }
        else if (stepLength >= 4 && stepLength <= 6) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6)
        }
        else if (stepLength >= 7 && stepLength <= 9) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9)
        }
        else if (stepLength >= 10 && stepLength <= 30) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30)
        }
        else if (stepLength > 30 && stepLength <= 60) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60)
        }
        else if (stepLength > 60) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60)
        }
      }

      calculateVisitLength(visitLength)
      calculateStepLength(stepLength)

      returnvalue
    }
    filteredSessionid2AggrInfoRDD



  }

  /**
    * 获取通过筛选条件的session的访问明细数据RDD
    */
  def getSessionid2detailRDD(filteredSessionid2AggrInfoRDD: RDD[(String, String)],sessionid2actionRDD: RDD[(String, Row)]): RDD[(String, Row)]={
    sessionid2actionRDD.join(filteredSessionid2AggrInfoRDD).map{tuple =>
        val sessionId = tuple._1
        val row = tuple._2._1
      (sessionId,row)
    }
  }

  def randomExtractSession(sc:SparkContext,taskid:java.lang.Long,filteredSessionid2AggrInfoRDD:RDD[(String,String)],sessionid2detailRDD:RDD[(String,Row)]):Unit={
    /**
      * 第一步，计算出每天每小时的session数量  // 获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
      */
    val countMap =filteredSessionid2AggrInfoRDD.map{ tuple=>
      val aggrInfo: String = tuple._2
      val startTime: String = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME)
      val dateHour: String = DateUtils.getDateHour(startTime)
      (dateHour,aggrInfo)
    }.countByKey()

    val dateHourCountMap: java.util.HashMap[String, java.util.HashMap[String, Long]] = new java.util.HashMap[String, java.util.HashMap[String, Long]]
    import scala.collection.JavaConversions._
    for (countEntry <- countMap.entrySet) {
      val dateHour: String = countEntry.getKey
      val date: String = dateHour.split("_")(0)
      val hour: String = dateHour.split("_")(1)
      val count: Long = java.lang.Long.valueOf(String.valueOf(countEntry.getValue))
      var hourCountMap: java.util.HashMap[String, Long] = dateHourCountMap.get(date)
      if (hourCountMap == null) {
        hourCountMap = new java.util.HashMap[String, Long]
        dateHourCountMap.put(date, hourCountMap)
      }
      hourCountMap.put(hour, count)
    }


    // 总共要抽取100个session，先按照天数，进行平分
    val extractNumberPerDay: Int = 100 / dateHourCountMap.size


//    myPrint(filteredSessionid2AggrInfoRDD,5)
  }


  def myPrint(rdd: RDD[_], num: Int): Unit = {
    rdd.take(num).foreach(println)
  }

  case class Person(date: String,
                    user_id: java.lang.Long,
                    session_id: String,
                    page_id: java.lang.Long,
                    action_time: String,
                    search_keyword: String,
                    click_category_id: java.lang.Long,
                    order_category_ids: String,
                    order_product_ids: String,
                    pay_category_ids: String,
                    pay_product_ids: String,
                    city_id: java.lang.Long)


}