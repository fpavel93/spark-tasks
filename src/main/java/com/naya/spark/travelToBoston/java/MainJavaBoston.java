package com.naya.spark.travelToBoston.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class MainJavaBoston {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("boston").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("src/main/resources/taxi_orders.txt");
        JavaRDD<String> rddDrivers = sc.textFile("src/main/resources/drivers.txt");

        TaxiOrdersService taxiOrdersService = new TaxiOrdersService();

        long lines = taxiOrdersService.numOfLinesInTxtFile(rdd);
        System.out.println("num of lines: " + lines);

        long num = taxiOrdersService.numOfTravelToBostonLongerThen10Km(rdd);
        System.out.println("num of t ravel to Boston longer then 10 km: " + num);

        double sum = taxiOrdersService.sumOfKmTravelToBoston(rdd);
        System.out.println("total sum of km travel to Boston: " + sum);

        List<Tuple2<String, Integer>> tuple2List = taxiOrdersService.most3LongTravelDrivers(rdd, rddDrivers);
        System.out.println("3 most driven drivers names and km\n\n" + tuple2List);
    }
}
