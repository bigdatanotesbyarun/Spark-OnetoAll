package com.spark.sql.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JoinExample {

	public static void main(String[] args) {
	
		SparkSession spark = SparkSession.builder().master("local[1]").config("Name", "Arun").appName("MySparkManager").getOrCreate();
		
		StructField[] fields=new StructField[] {
				new StructField("name", DataTypes.StringType, true, Metadata.empty()),
				new StructField("custid", DataTypes.IntegerType, true, Metadata.empty()),
				new StructField("age", DataTypes.IntegerType, true, Metadata.empty())
			};
		StructType schema=new StructType(fields);
		
		StructField[] fields1=new StructField[] {
				new StructField("custid", DataTypes.IntegerType, true, Metadata.empty()),
				new StructField("orderID", DataTypes.StringType, true, Metadata.empty()),
				new StructField("orderName", DataTypes.StringType, true, Metadata.empty())
			};
		StructType schema1=new StructType(fields1);
		
		List<Row> data1=Arrays.asList(
				RowFactory.create("Arun",101,35),
				RowFactory.create("Rishika",102,30),
				RowFactory.create("Chiyu",103,7));

		List<Row> data2=Arrays.asList(
				RowFactory.create(101,"order1","Cofee"),
				RowFactory.create(103,"order2","Colddrink"));
		
		Dataset<Row> ds3=spark.createDataset(data1,RowEncoder.apply(schema));
		Dataset<Row> ds4=spark.createDataset(data2,RowEncoder.apply(schema1));
		
		ds3.show();
		ds4.show();
		Column joinCondition=ds3.col("custid").equalTo(ds4.col("custid"));
		
		ds3.join(ds4,joinCondition,"fullOuter").show();
		/*
		
		+-------+------+---+------+-------+---------+
		|   name|custid|age|custid|orderID|orderName|
		+-------+------+---+------+-------+---------+
		|   Arun|   101| 35|   101| order1|    Cofee|
		|Rishika|   102| 30|  null|   null|     null|
		|  Chiyu|   103|  7|   103| order2|Colddrink|
		+-------+------+---+------+-------+---------+  */
		
		ds3.join(ds4,joinCondition,"leftOuter").show();
		/*

		+-------+------+---+------+-------+---------+
		|   name|custid|age|custid|orderID|orderName|
		+-------+------+---+------+-------+---------+
		|   Arun|   101| 35|   101| order1|    Cofee|
		|Rishika|   102| 30|  null|   null|     null|
		|  Chiyu|   103|  7|   103| order2|Colddrink|
		+-------+------+---+------+-------+---------+
		 */
		
		
		
		ds3.join(ds4,joinCondition,"rightOuter").show();
		/*
		+-----+------+---+------+-------+---------+
		| name|custid|age|custid|orderID|orderName|
		+-----+------+---+------+-------+---------+
		| Arun|   101| 35|   101| order1|    Cofee|
		|Chiyu|   103|  7|   103| order2|Colddrink|
		+-----+------+---+------+-------+---------+
		 
		 */
		
		ds3.join(ds4,joinCondition,"leftAnti").show();
		/*
		+-------+------+---+
		|   name|custid|age|
		+-------+------+---+
		|Rishika|   102| 30|
		+-------+------+---+ 
		*/
			
		ds3.join(ds4,joinCondition,"cross").show();
		/*
		+-------+------+---+
		|   name|custid|age|
		+-------+------+---+
		|Rishika|   102| 30|
		+-------+------+---+
		*/
		ds3.crossJoin(ds4).show();
		
		/*
		+-------+------+---+------+-------+---------+
		|   name|custid|age|custid|orderID|orderName|
		+-------+------+---+------+-------+---------+
		|   Arun|   101| 35|   101| order1|    Cofee|
		|   Arun|   101| 35|   103| order2|Colddrink|
		|Rishika|   102| 30|   101| order1|    Cofee|
		|Rishika|   102| 30|   103| order2|Colddrink|
		|  Chiyu|   103|  7|   101| order1|    Cofee|
		|  Chiyu|   103|  7|   103| order2|Colddrink|
		+-------+------+---+------+-------+---------+ 
		*/

		//'inner', 'outer', 
		//'full', 'fullouter', 'full_outer',
		//'leftouter', 'left', 'left_outer',
		//'rightouter', 'right', 'right_outer',
		//'leftsemi', 'left_semi', 'semi',
		//'leftanti', 'left_anti', 'anti', 'cross'.
	}
}
