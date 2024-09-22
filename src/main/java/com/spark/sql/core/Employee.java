package com.spark.sql.core;

import java.io.Serializable;

public class Employee implements Serializable{

	private String name;
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Employee() {
	}

	public Employee(String name) {
		super();
		this.name = name;
	}

}
