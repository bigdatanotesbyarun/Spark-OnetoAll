package com.spark.sql.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkHiveIntegration {

	public static void main(String[] args) {
		System.out.println("Hello");
		SparkSession spark = SparkSession.builder().master("yarn").appName("MySparkManager").enableHiveSupport().getOrCreate();
	/*	spark.sql("create database if not exists gfolynrf_standardization");
		//spark.sql("drop table if  exists gfolynrf_standardization.om_account_dim_actv");
		spark.sql("create external table if not exists gfolynrf_standardization.om_account_dim_actv (acti bigint , account_sk String) location '/user/hive/warehouse/gfolynrf_standardization.db/om_account_dim_actv'");
		spark.sql("describe formatted gfolynrf_standardization.om_account_dim_actv").show();
		*/spark.sql("insert into gfolynrf_standardization.om_account_dim_actv values (111, 'Hello')").show();
		/*
		
		Dataset<Row> ds=spark.sql("select * from gfolynrf_standardization.om_account_dim_actv");
		HashMap<String,String> a=new HashMap();
		a.put("Query1", "select * from gfolynrf_standardization.om_account_dim_actv");
		a.put("Query2", "select * from gfolynrf_standardization.om_account_dim_actv");
		a.put("Query3", "select * from gfolynrf_standardization.om_account_dim_actv");
		a.put("Query4", "select * from gfolynrf_standardization.om_account_dim_actv");
		a.put("Query5", "select * from gfolynrf_standardization.om_account_dim_actv");
		a.put("Query6", "select * from gfolynrf_standardization.om_account_dim_actv");
		a.put("Query7", "select * from gfolynrf_standardization.om_account_dim_actv");
		ExecutorService ex=Executors.newFixedThreadPool(10);
		System.out.println("Hello");
		for (Map.Entry<String,String> b :a.entrySet()) {
			Runnable task=()->{
				System.out.println("SHOW");
				spark.sql("select * from gfolynrf_standardization.om_account_dim_actv").show();
				System.out.println(spark.sql("select * from gfolynrf_standardization.om_account_dim_actv").count());};
				ex.submit(task);
				System.out.println("SHOWOver");
		}
		ex.shutdown();
		   
*/
	}

}
