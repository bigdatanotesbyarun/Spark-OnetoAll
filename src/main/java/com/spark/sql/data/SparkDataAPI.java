package com.spark.sql.data;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.annotation.JsonCreator.Mode;

public class SparkDataAPI {

	
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder().master("local[1]").config("Name", "Arun").appName("MySparkManager").getOrCreate();
		SparkContext sc=spark.sparkContext();
		JavaSparkContext jsc=new JavaSparkContext(sc);
		
		//S.RF OS
		Dataset<Row> df=spark.read().format("").option("", "").schema("").csv("");
		Dataset<Row> i2=spark.read().format("").option("", "").schema("").json("");
		Dataset<Row> i3=spark.read().format("").option("", "").schema("").parquet("");
		Dataset<Row> i5=spark.read().format("").option("", "").schema("").text("");
		Dataset<Row> i4=spark.read().format("").option("", "").schema("").jdbc("url", null, null);
		Dataset<Row> i6=spark.read().format("").option("", "").schema("").table("");
		Dataset<Row> i7=spark.read().format("").option("", "").schema("").load();
		Dataset<Row> i8=spark.read().format("").option("", "").schema("").orc("");
		
		//DF.WFMO
		
		df.write().format("").mode(SaveMode.Append).option("", "").save("hdfspath");
		df.write().format("").mode(SaveMode.Append).option("", "").saveAsTable("NewTableName");
		df.write().format("").mode(SaveMode.Append).option("", "").insertInto("Existing-tableName");
		df.write().format("").mode(SaveMode.Append).option("", "").partitionBy("").save("tableName");
		df.write().format("").mode(SaveMode.Append).option("", "").csv("path");
		df.write().format("").mode(SaveMode.Append).option("", "").json("path");
		df.write().format("").mode(SaveMode.Append).option("", "").parquet("path");
		df.write().format("").mode(SaveMode.Append).option("", "").jdbc("", "", null);
		df.write().format("").mode(SaveMode.Append).option("", "").text("path");
	
		
	}
}
