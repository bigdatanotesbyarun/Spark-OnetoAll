package com.spark.sql.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CreateDataSet {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[1]").config("Name", "Arun").appName("MySparkManager").getOrCreate();
		List<Employee> list=new ArrayList<>();
		list.add(new Employee("Arun"));
		Dataset<Employee> ds=spark.createDataset(list, Encoders.bean(Employee.class));
		
	
		
		List<String> listString=Arrays.asList("a","b","c");
		Dataset<String> ds1=spark.createDataset(listString, Encoders.STRING());
		
		
		
		StructField[] fields=new StructField[] {
				new StructField("name", DataTypes.StringType, true, Metadata.empty()),
				new StructField("state", DataTypes.StringType, true, Metadata.empty())
		};
			
			StructType schema=new StructType(fields);
			
			List<Row> listOfRow=Arrays.asList(
					RowFactory.create("Arun","Up"),
					RowFactory.create("Arun1","MP"),
					RowFactory.create("Arun3","CG"));
			
			Dataset<Row> ds3=spark.createDataset(listOfRow,RowEncoder.apply(schema));
			ds3.show();
	}
	
	
	
}
