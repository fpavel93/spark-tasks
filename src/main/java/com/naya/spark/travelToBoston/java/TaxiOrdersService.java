package com.naya.spark.travelToBoston.java;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TaxiOrdersService {

    public long numOfLinesInTxtFile(JavaRDD<String> rdd){
       return rdd.count();
    }

    public long numOfTravelToBostonLongerThen10Km(JavaRDD<String> rdd){
        return rdd.map(s -> s.split(" "))
                .filter(strings -> strings[1].equals("Boston") && Integer.parseInt(strings[2]) > 10)
                .count();
    }

    public double sumOfKmTravelToBoston(JavaRDD<String> rdd){
        return rdd.map(s -> s.split(" "))
                .filter(strings -> strings[1].equals("Boston"))
                .mapToDouble(strings -> Integer.parseInt(strings[2]))
                .sum();
    }

    public Map<String, Integer> most3LongTravelDrivers(JavaRDD<String> rddOrders, JavaRDD<String> rddDrivers){
        return rddOrders.map(s -> s.split(" "))
                .mapToPair(strings -> Tuple2.apply(strings[0],Integer.parseInt(strings[2])))
                .reduceByKey(Integer::sum)
                //.groupByKey()
                //.mapToPair(tuple -> Tuple2.apply(tuple._1,sumOfIntegers(tuple._2)))
                .join(rddDrivers.map(s -> s.split(", "))
                        .mapToPair(strings -> Tuple2.apply(strings[0],strings[1])))
                .mapToPair(tuple -> Tuple2.apply(tuple._2._1,tuple._2._2))
                .sortByKey(false)
                .mapToPair(Tuple2::swap)
                .take(3)
                .stream()
                .collect(Collectors.toMap(o -> o._1,o -> o._2));
    }

    private static Integer sumOfIntegers(Iterable<Integer> integers) {
        int sum = 0;
        for (Integer integer : integers) {
            sum = sum + integer;
        }
        return sum;
    }
}
