package com.spark.sql.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CreateDataFrame {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[1]").config("Name", "Arun").appName("MySparkManager").getOrCreate();
		SparkContext sc=spark.sparkContext();
		JavaSparkContext jsc=new JavaSparkContext(sc);
		
		
		
		/*spark.createDataFrame*/
		
		List<Employee> a=new ArrayList<>();
		a.add(new Employee("Arun"));
		JavaRDD<Employee> personRDD=jsc.parallelize(a);
		Dataset<Row> ds=spark.createDataFrame(personRDD, Employee.class);
		ds.show();
		Dataset<Row> ds1=spark.createDataFrame(a, Employee.class);
		ds1.show();
		
		StructField[] fields=new StructField[] {
				new StructField("name", DataTypes.StringType, true, Metadata.empty())
			};
			
		StructType schema=new StructType(fields);
		
		List<Row> listOfRow=Arrays.asList(
				RowFactory.create("Arun"),
				RowFactory.create("Arun1"),
				RowFactory.create("Arun3"));
		
		
		Dataset<Row> ds3=spark.createDataFrame(listOfRow,schema);
		ds3.show();
		
		JavaRDD<Row> javaRDDRow=jsc.parallelize(listOfRow);
		Dataset<Row> ds4=spark.createDataFrame(javaRDDRow,schema);
		ds4.show();
	}
}
