package com.spark.sql.hive;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.datanucleus.store.types.wrappers.Date;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class SparkHiveIntegrationPartitionBy {

	static SparkSession spark = SparkSession.builder().master("local[2]").appName("MySparkManager").enableHiveSupport().getOrCreate();
	static String tblName="gfolynrf_standardization.om_gemfire_control_jobs_last_excution_ts";
	
	public static void main(String[] args) {
		spark.sql("create database if not exists gfolynrf_standardization");
		//spark.sql("drop  table if  exists gfolynrf_standardization.om_gemfire_control_jobs_last_excution_ts");
		spark.sql("create  table if not exists gfolynrf_standardization.om_gemfire_control_jobs_last_excution_ts"
				+ "(jobname String ,start_execution_ts timestamp,last_execution_ts timestamp)"
				+ "partitioned by (state String,zipCode int)"
				+ " stored as parquet"
				+ "");
		
		spark.sql("select * from gfolynrf_standardization.om_gemfire_control_jobs_last_excution_ts").show();
		
		Timestamp end=new Timestamp(Date.from(Instant.now()).getTime());
		Timestamp ts=new Timestamp(Date.from(Instant.now()).getTime());
		updateTs(ts,end);
	}
	
	
	public static void updateTs(Timestamp starts, Timestamp endts) {
		List<JobMetaData> list=new ArrayList<JobMetaData>();
		JobMetaData md=new JobMetaData();
		md.setJobName("SKRecon");
        md.setStart_execution_ts(starts);
        md.setLast_execution_ts(endts);
        md.setState("UP");
        md.setZipCode(2222);
        list.add(md);
        
		JobMetaData md1=new JobMetaData();
		md1.setJobName("SKRecon");
        md1.setStart_execution_ts(starts);
        md1.setLast_execution_ts(endts);
        md1.setState("UP1");
        md1.setZipCode(1111);
        list.add(md1);
        
        Dataset<Row> metaDataDs=spark.createDataFrame(list, JobMetaData.class);
        metaDataDs.selectExpr(spark.read().table(tblName).schema().fieldNames()).write().mode(SaveMode.Append).insertInto(tblName);
	}
}
