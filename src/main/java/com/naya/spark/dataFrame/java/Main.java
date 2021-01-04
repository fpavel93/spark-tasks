package com.naya.spark.dataFrame.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("dataFrameHW")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = sparkSession.read().json("src/main/java/com/naya/spark/dataFrame/data/profiles.json");

        ProfileServiceSpark profileServiceSpark = new ProfileServiceSpark();

        profileServiceSpark.printTable(dataset);
        profileServiceSpark.printSchema(dataset);
        profileServiceSpark.printTypes(dataset);
        Dataset<Row> dataset1 = profileServiceSpark.addColSalary(dataset);
        dataset1.show();
        Dataset<Row> programmers = profileServiceSpark.findProgrammersWithMostPopularTechnologyWithSalaryLower1200(dataset1);
        programmers.show();
    }
}
