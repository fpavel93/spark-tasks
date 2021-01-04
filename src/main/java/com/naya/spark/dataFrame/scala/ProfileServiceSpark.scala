package com.naya.spark.dataFrame.scala

import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import org.apache.spark.sql.functions._

class ProfileServiceSpark {

  def printTable(dataFrame: DataFrame):Unit={
    dataFrame.show()
  }

  def printSchema(dataFrame: DataFrame):Unit= {
    dataFrame.printSchema()
  }

  def printTypes(dataFrame: DataFrame):Unit= {
    val dtypes = dataFrame.dtypes
    dtypes.foreach(println(_))
  }

  def addColSalary(dataFrame: DataFrame):DataFrame= {
    dataFrame.withColumn("salary", col("age") * size(col("keywords")) * 10)
  }

  def findProgrammersWithMostPopularTechnologyWithSalaryLower1200(dataFrame: DataFrame):DataFrame= {
    val dataFrame1 = dataFrame.withColumn("tech", explode(col("keywords")))
      .persist()

    val mostPopular = dataFrame1.select(col("tech"))
      .groupBy("tech").count.sort(desc("count"))
      .first.get(0)

    println(mostPopular)

    dataFrame1.filter(col("tech").equalTo(mostPopular))
      .filter(col("salary").leq(1200))
      .select("name")
  }
}
