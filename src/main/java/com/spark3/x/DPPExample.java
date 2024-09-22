package com.spark3.x;

import java.util.Arrays;
import java.util.Scanner;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;


public class DPPExample {

    public static void main(String[] args) { SparkSession spark = SparkSession.builder()
            .appName("Dynamic Partition Pruning Example")
            .master("local")
            .getOrCreate();

    spark.conf().set("spark.sql.adaptive.enabled", true);
    spark.conf().set("spark.sql.dynamicPartitionPruning.enabled", true);

    Dataset<Row> sales = spark.createDataFrame(Arrays.asList(
            new SalesData("2022-01-01", "North", 100),
            new SalesData("2022-01-02", "South", 200),
            new SalesData("2022-01-03", "North", 300),
            new SalesData("2022-01-04", "South", 400)
    ), SalesData.class).repartitionByRange(4, functions.col("date"));

    sales.cache();

    sales.createOrReplaceTempView("sales");
    
    //spark.sql("CREATE TEMP VIEW sales AS SELECT * FROM salestable");

    //spark.sql("ANALYZE TABLE sales COMPUTE STATISTICS");*/

    Dataset<Row> result = spark.sql("SELECT /*+ AQE, PRUNE */ region, SUM(sales) FROM sales WHERE date > '2022-01-02' GROUP BY region");

    result.explain(true);

    result.show();}
}
