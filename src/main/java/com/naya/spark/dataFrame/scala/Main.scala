package com.naya.spark.dataFrame.scala

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("dataFrameHW")
      .master("local[*]")
      .getOrCreate()

    val dataFrame = sparkSession.read.json("src/main/java/com/naya/spark/dataFrame/data/profiles.json")

    val profileServiceSpark = new ProfileServiceSpark

    profileServiceSpark.printTable(dataFrame)
    profileServiceSpark.printSchema(dataFrame)
    profileServiceSpark.printTypes(dataFrame)
    val dataFrame1 = profileServiceSpark.addColSalary(dataFrame)
    dataFrame1.show()
    val dataFrame2 = profileServiceSpark.findProgrammersWithMostPopularTechnologyWithSalaryLower1200(dataFrame1)
    dataFrame2.show()
  }
}
