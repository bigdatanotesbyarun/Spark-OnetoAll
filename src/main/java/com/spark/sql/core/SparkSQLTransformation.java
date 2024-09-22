package com.spark.sql.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkSQLTransformation {

	public SparkSQLTransformation() {
	}
	
	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().master("local[1]").config("Name", "Arun").appName("MySparkManager").getOrCreate();
		
		
		
	}
}
