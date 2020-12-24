package com.naya.spark.friday13;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import java.time.DayOfWeek;
import java.time.LocalDate;

public class Friday13SparkService {

    public void countFriday13InYears(JavaRDD<LocalDate> rdd){

        rdd.filter(localDate -> localDate.getDayOfWeek() == DayOfWeek.FRIDAY)
                .groupBy(LocalDate::getYear)
                .mapToPair(tuple -> Tuple2.apply(tuple._1, Iterables.size(tuple._2)))
                .collectAsMap()
                .entrySet().stream()
                .sorted((o1, o2) -> (int)(o2.getValue()-o1.getValue()))
                .forEach(entry -> System.out.println(entry.getKey()+" "+entry.getValue()));
    }
}
