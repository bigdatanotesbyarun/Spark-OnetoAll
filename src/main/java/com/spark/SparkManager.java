package com.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkManager {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[1]").config("Name", "Arun").appName("MySparkManager").getOrCreate();
		SparkContext sc=spark.sparkContext();
		JavaSparkContext jsc=new JavaSparkContext(sc);
		List<String> list=Arrays.asList(new String[] {"A,B","B"});
		JavaRDD<String> rdd=jsc.parallelize(list);
		rdd.map(s->s.toUpperCase()).count();
	}
}
