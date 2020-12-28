package com.naya.spark.travelToBoston.scala

import org.apache.spark.SparkContext

object MainScalaBoston {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(master = "local", appName = "boston")

    val rdd = sc.textFile("src/main/resources/taxi_orders.txt")
    val rddDrivers = sc.textFile("src/main/resources/drivers.txt")

    val taxiOrdersService = new ScalaTaxiOrdersService

    val lines: Long = taxiOrdersService.numOfLinesInTxtFile(rdd)
    println("num of lines: " + lines)

    val num: Long = taxiOrdersService.numOfTravelToBostonLongerThen10Km(rdd)
    println("num of t ravel to Boston longer then 10 km: " + num)

    val sum: Double = taxiOrdersService.sumOfKmTravelToBoston(rdd)
    println("total sum of km travel to Boston: " + sum)

    val map = taxiOrdersService.most3LongTravelDrivers(rdd, rddDrivers)
    println("3 most driven drivers names and km\n\n" + map)
  }
}
