package com.spark.sql.func;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import java.util.stream.IntStream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.RowNumber;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.util.LongAccumulator;

public class WindowFunctionExample {
	
	public static void main(String[] args) {
		
		List<Person> personList=Arrays.asList(new Person("Arun", 20),new Person("Rishika", 20));
		SparkSession spark=SparkSession.builder().master("local[1]").getOrCreate();
		Dataset<Person> ds=spark.createDataset(personList, Encoders.bean(Person.class));
		WindowSpec ws=Window.partitionBy("age").orderBy("name");
		/*ds=ds.withColumn("row_number",row_number().over(ws));
		ds=ds.filter(col("row_numer").equalsTo(new Integer(1)));
		*/
		
		JavaSparkContext jsc=new JavaSparkContext(spark.sparkContext());
		Broadcast<List<Person>> b =jsc.broadcast(personList);
		jsc.parallelize(Arrays.asList(1,2,3)).map(s-> s+b.value().get(0).getFirstName());
		
		
		LongAccumulator i=spark.sparkContext().longAccumulator("Sum");
		jsc.parallelize(Arrays.asList(1,2,3)).map(s->{
			 i.add(1111); 
			 return 1;
		});
		
		
	}
	

}




class Person implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String firstName;
	private String lastName;
	private String key;
	private int age;

	public Person() {
		
	}
	public Person(String key, int age) {
		this.key=key;
		this.age=age;
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
	public int getage() {
		return age;
	}
	public void setage(int age) {
		this.age = age;
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