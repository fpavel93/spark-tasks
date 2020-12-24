package com.naya.spark.friday13;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.LocalDate;
import java.time.Month;
import java.time.Period;
import java.util.List;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("friday13").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Friday13SparkService friday13SparkService = new Friday13SparkService();

        LocalDate startDate = LocalDate.of(1900, Month.JANUARY,13);
        LocalDate endDate = LocalDate.of(1920, Month.DECEMBER, 14);

       /* List<LocalDate> dates = Stream.iterate(startDate, localDate -> localDate.plusMonths(1))
                .limit(ChronoUnit.MONTHS.between(startDate, endDate))
                .collect(Collectors.toList());*/

        List<LocalDate> dates = startDate.datesUntil(endDate, Period.ofMonths(1))
                .collect(Collectors.toList());

        JavaRDD<LocalDate> rdd = sc.parallelize(dates);

        friday13SparkService.countFriday13InYears(rdd);

        sc.close();
    }
}
