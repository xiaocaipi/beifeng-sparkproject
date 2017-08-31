package com.ibeifeng.sparkproject

import com.ibeifeng.sparkproject.util.DateUtils

/**
  * Created by caidanfeng733 on 8/23/17.
  */
object test11 {

  def main(args: Array[String]) {
    val tuple = ("20170822_487_0",1)
    val keySplited: Array[String] = tuple._1.split("_")
    val date: String = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))
    print(date)
  }
}
