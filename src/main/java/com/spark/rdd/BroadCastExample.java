package com.spark.rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

public class BroadCastExample {
	
	public static void main(String[] args) {
		
		SparkSession spark=SparkSession.builder().master("local[1]").appName("BroadCastExample").getOrCreate();
		List<Integer> list=Arrays.asList(1,2,3);
		JavaSparkContext jsc=new JavaSparkContext(spark.sparkContext());
		LongAccumulator countAccum=spark.sparkContext().longAccumulator("count");
		LongAccumulator timeAccum=spark.sparkContext().longAccumulator("time");
		DoubleAccumulator sumAccum=spark.sparkContext().doubleAccumulator("Sum");
		Broadcast<List<Integer>> blist=jsc.broadcast(list);
		JavaRDD<List<Integer>> rdd=jsc.parallelize(Arrays.asList(list));
		JavaRDD<Integer> rdd1=rdd.map(s -> blist.value().get(0));
		rdd1.foreach(s->{
			long startts=System.currentTimeMillis();
			countAccum.add(1);
			sumAccum.add(s);
			timeAccum.add(System.currentTimeMillis()-startts);
			System.out.println(s);	
		});
		System.out.println(countAccum);
		System.out.println(timeAccum);
		System.out.println(sumAccum);
	}
}
