package com.naya.spark.travelToBoston.scala

import org.apache.spark.rdd.RDD

class ScalaTaxiOrdersService {

  def numOfLinesInTxtFile(rdd: RDD[String]): Long = rdd.count

  def numOfTravelToBostonLongerThen10Km(rdd: RDD[String]): Long ={
    rdd.map((s: String) => s.split(" "))
      .filter((strings: Array[String]) => strings(1) == "Boston" && strings(2).toInt > 10)
      .count
  }

  def sumOfKmTravelToBoston(rdd: RDD[String]): Double = {
    rdd.map((s: String) => s.split(" "))
      .filter((strings: Array[String]) => strings(1) == "Boston")
      .map((strings: Array[String]) => strings(2).toInt)
      .sum
  }

  def most3LongTravelDrivers(rddOrders: RDD[String], rddDrivers: RDD[String]): Map[String, Int] = {
    val driversIdNames = rddDrivers.map((s: String) => s.split(", "))
      .map((strings: Array[String]) => (strings(0), strings(1)))

    rddOrders.map((s: String) => s.split(" "))
      .map((strings: Array[String]) => (strings(0), strings(2).toInt))
      .reduceByKey(_+_)
      //.groupByKey
      //.map(tuple => (tuple._1, tuple._2.sum))
      .join(driversIdNames)
      .map((tuple: (String, (Int, String))) => (tuple._2._1, tuple._2._2))
      .sortByKey(ascending = false)
      .map(_.swap)
      .take(3)
      .toMap
  }
}
