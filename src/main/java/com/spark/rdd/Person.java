package com.spark.rdd;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.arrow.flatbuf.Int;

public class Person implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String firstName;
	private String lastName;
	private String key;
	private int value;

	public Person() {
		
	}
	public Person(String key, int value) {
		this.key=key;
		this.value=value;
	}

	public Person(String firstName, String lastName) {
		this.firstName=firstName;
		this.lastName=lastName;
	}

	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public int getValue() {
		return value;
	}
	public void setValue(int value) {
		this.value = value;
	}
	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	
	public static void main(String[] args) {
		int[] e= {};
		IntStream d=IntStream.of(e);
		System.out.println(d.reduce(0,(a,b)->a+b));
		System.out.println(IntStream.range(1, 3).reduce(5,(a,b)->a+b));
	}
}
