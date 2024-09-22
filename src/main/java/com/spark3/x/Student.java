package com.spark3.x;

public class Student {
	
	private String department;
	private String studentName;
	private int age;
	public Student(String department, String studentName, int age) {
		super();
		this.department = department;
		this.studentName = studentName;
		this.age = age;
	}
	public String getDepartment() {
		return department;
	}
	public void setDepartment(String department) {
		this.department = department;
	}
	public String getStudentName() {
		return studentName;
	}
	public void setStudentName(String studentName) {
		this.studentName = studentName;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	
	
}
