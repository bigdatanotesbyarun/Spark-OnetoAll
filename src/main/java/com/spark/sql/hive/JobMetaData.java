package com.spark.sql.hive;

import java.sql.Timestamp;

public class JobMetaData {
	
	String jobName;
	
	Timestamp start_execution_ts;
	
	Timestamp last_execution_ts;
	
	String state;
	
	int zipCode;

	public int getZipCode() {
		return zipCode;
	}

	public void setZipCode(int zipCode) {
		this.zipCode = zipCode;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public Timestamp getStart_execution_ts() {
		return start_execution_ts;
	}

	public void setStart_execution_ts(Timestamp start_execution_ts) {
		this.start_execution_ts = start_execution_ts;
	}

	public Timestamp getLast_execution_ts() {
		return last_execution_ts;
	}

	public void setLast_execution_ts(Timestamp last_execution_ts) {
		this.last_execution_ts = last_execution_ts;
	}

	
}
