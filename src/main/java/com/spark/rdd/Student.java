package com.spark.rdd;

import java.io.Serializable;
import java.util.List;

public class Student implements Serializable{

	private String name;

	private List<String> subjects;

	public Student(String name, List<String> subjects) {
		super();
		this.name = name;
		this.subjects = subjects;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<String> getSubjects() {
		return subjects;
	}

	public void setSubjects(List<String> subjects) {
		this.subjects = subjects;
	}

	public List<String> getAllSubjects() {
		return subjects;
	}
}