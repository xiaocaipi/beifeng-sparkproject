package com.ibeifeng.sparkproject

import com.ibeifeng.sparkproject.constant.Constants
import com.ibeifeng.sparkproject.util.StringUtils
import org.apache.spark.AccumulatorParam

/**
  * Created by caidanfeng733 on 8/17/17.
  */
object MySessionAggrStatAccumulator extends AccumulatorParam[String] {

  def zero(initialValue:String):String = {
    Constants.SESSION_COUNT + "=0|" + Constants.TIME_PERIOD_1s_3s + "=0|" + Constants.TIME_PERIOD_4s_6s + "=0|" + Constants.TIME_PERIOD_7s_9s + "=0|" + Constants.TIME_PERIOD_10s_30s + "=0|" + Constants.TIME_PERIOD_30s_60s + "=0|" + Constants.TIME_PERIOD_1m_3m + "=0|" + Constants.TIME_PERIOD_3m_10m + "=0|" + Constants.TIME_PERIOD_10m_30m + "=0|" + Constants.TIME_PERIOD_30m + "=0|" + Constants.STEP_PERIOD_1_3 + "=0|" + Constants.STEP_PERIOD_4_6 + "=0|" + Constants.STEP_PERIOD_7_9 + "=0|" + Constants.STEP_PERIOD_10_30 + "=0|" + Constants.STEP_PERIOD_30_60 + "=0|" + Constants.STEP_PERIOD_60 + "=0";
  }


  /**
    * 其次需要实现一个累加方法
    * @param v1
    * @param v2
    * @return
    */
  def addInPlace(v1:String,v2:String):String  = {

    /**
      * 如果初始值为空,那么返回v2
      */
    if (v1 == "") {
      v2
    } else {

      /**
        * 从现有的连接串中提取v2所对应的值
        */
      val oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2)

      /**
        * 累加1
        */
      val newValue = Integer.valueOf(oldValue) + 1

      /**
        * 改链接串中的v2设置新的累加后的值
        */
      StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue))
    }
  }
}
