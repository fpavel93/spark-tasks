package com.naya.spark.dataFrame.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class ProfileServiceSpark {

    public void printTable(Dataset<Row> dataset){
        dataset.show();
    }

    public void printSchema(Dataset<Row> dataset){
        dataset.printSchema();
    }

    public void printTypes(Dataset<Row> dataset){
        Tuple2<String, String>[] dtypes = dataset.dtypes();
        Arrays.stream(dtypes).forEach(System.out::println);
    }

    public Dataset<Row> addColSalary(Dataset<Row> dataset){
        return dataset.withColumn("salary", col("age").multiply(size(col("keywords")).multiply(10)));
    }

    public Dataset<Row> findProgrammersWithMostPopularTechnologyWithSalaryLower1200(Dataset<Row> dataset){
        Dataset<Row> persist = dataset.withColumn("tech", explode(col("keywords")))
                .persist();

        String mostPopular = persist.select(col("tech"))
                .groupBy("tech")
                .count()
                .sort(desc("count"))
                .first().get(0).toString();

        System.out.println(mostPopular);

        return persist.filter(col("tech").equalTo(mostPopular))
                .filter(col("salary").leq(1200))
                .select("name");
    }
}
