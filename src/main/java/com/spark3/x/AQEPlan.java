package com.spark3.x;

import static org.apache.spark.sql.functions.col;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AQEPlan {

	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder().master("local[1]").config("spark.sql.adaptive.enabled", true).appName("MySparkManager").getOrCreate();
		List<Student> listIT=IntStream.range(0, 10).mapToObj(i-> new Student("IT","Arun",20)).limit(1).collect(Collectors.toList());
		List<Student> listCS=IntStream.range(0, 10).mapToObj(i-> new Student("CS","Rishika",25)).limit(1).collect(Collectors.toList());
		List<Student> listEC=IntStream.range(0, 10).mapToObj(i-> new Student("EC","Aarna",30)).limit(1).collect(Collectors.toList());
		List<Student> listEC1=IntStream.range(0, 10).mapToObj(i-> new Student(null,"Aarna",30)).limit(1).collect(Collectors.toList());
		List<Department> listDepartMent=Arrays.asList(new Department("IT"),new Department("CS"),new Department("EC"),new Department(null));
		listIT.addAll(listCS);
		listIT.addAll(listEC);
		listIT.addAll(listEC1);
		Dataset<Student> ds=spark.createDataset(listIT, Encoders.bean(Student.class));
		ds.show();
		ds=ds.filter((col("department").isin("EC","IT").or(col("department").isNull())));
		ds.show();
		//Dataset<Department> dsDepart=spark.createDataset(listDepartMent, Encoders.bean(Department.class));
		//dsDepart.show();
		//ds.show();
		/*Dataset<Student> ds1=ds.filter(col("age").gt(new Integer(19)));
		Dataset<Row> ds2=ds1.join(dsDepart, ds1.col("department").equalTo(dsDepart.col("depart")) ,"inner").groupBy("department").count();
		ds2.show();
		System.out.println("DS2");
		ds2=ds2.filter((col("department").isin("EC","IT",null).or(col("department").isNull())));
		ds2.show();
		dsDepart.show();
		ds.show();*/
	}

	/*
	
	== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[department#4], functions=[count(1)])
   +- Exchange hashpartitioning(department#4, 200), ENSURE_REQUIREMENTS, [plan_id=28]
      +- HashAggregate(keys=[department#4], functions=[partial_count(1)])
         +- Project [department#4]
            +- BroadcastHashJoin [department#4], [depart#11], Inner, BuildRight, false
               :- LocalTableScan [department#4]
               +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=23]
                  +- LocalTableScan [depart#11]
	                  
	                  
	           == Physical Plan ==
*(2)            HashAggregate(keys=[department#4], functions=[count(1)])
                +- Exchange hashpartitioning(department#4, 200), ENSURE_REQUIREMENTS, [plan_id=34]
                +- *(1) HashAggregate(keys=[department#4], functions=[partial_count(1)])
                +- *(1) Project [department#4]
                +- *(1) BroadcastHashJoin [department#4], [depart#11], Inner, BuildRight, false
                :- *(1) LocalTableScan [department#4]
                +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=22]
               +- LocalTableScan [depart#11]
	*/
	}
