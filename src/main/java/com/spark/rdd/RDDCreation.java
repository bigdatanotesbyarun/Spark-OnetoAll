package com.spark.rdd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class RDDCreation {
	
	public static void main(String[] args) {
		
		//From Parallelize Method
		List<String> list=Arrays.asList(new String[] {"A,B","B"});
		SparkSession spark=SparkSession.builder().master("local[2]").appName("BroadCastExample").getOrCreate();
		JavaSparkContext jsc=new JavaSparkContext(spark.sparkContext());
		JavaRDD<String> rdd=jsc.parallelize(list);
		rdd.map(s->s.toUpperCase());
		
		//From TextFile Method
		JavaRDD<String> rdd1=jsc.textFile("C:\\WorkSpace\\SparkRepository\\SparkOnetoAll\\pom.xml");
		System.out.println(rdd1.collect());
		
		//From EmptyRDD Method
		JavaRDD<String> rdd2= jsc.emptyRDD();

		rdd.map(s->s.toUpperCase());
		
		rdd.flatMap(s->{
			List<String> result=new ArrayList();
			Iterator<String> i=Arrays.asList(s.split(".")).iterator();
			while (i.hasNext()) {
				String string = (String) i.next();
				result.add(string);
			}
			return result.iterator();
		});
		
		rdd.mapPartitions(itr->{
			List<String> result=new ArrayList();
			while (itr.hasNext()) {
				String string = (String) itr.next();
				result.add(string);
			}
			return result.iterator();
		});
	
		rdd.filter(s->s.equalsIgnoreCase("matching"));
		
		Arrays.asList(1,2,3,4,5);
		Arrays.asList("A","B","C");
		Arrays.asList(new Person(),new Person());
		
		
		JavaPairRDD<String, Integer> rddT=jsc.parallelizePairs(Arrays.asList(
				new Tuple2<>("Arun1",20),
				new Tuple2<>("Arun2",20),
				new Tuple2<>("Arun3",20),
				new Tuple2<>("Arun4",20),
				new Tuple2<>("Arun5",20),
				new Tuple2<>("Arun6",20)
				));
		
		JavaPairRDD<String, Integer> rddR=jsc.parallelizePairs(Arrays.asList(
				new Tuple2<>("Arun1",50),
				new Tuple2<>("Rishika1",20),
				new Tuple2<>("Rishika1",20),
				new Tuple2<>("Rishika1",20),
				new Tuple2<>("Rishika5",20),
				new Tuple2<>("Rishika6",20)
				));
		
		JavaPairRDD<String, Integer> rddT1=rddT.reduceByKey((a,b)->a+b);
		System.out.println("Collection After Reducing");
		System.out.println(rddT1.collect());
		
		JavaPairRDD<String, Iterable<Integer>> rddT2=rddT.groupByKey();
		System.out.println("Collection After Grouping");
		System.out.println(rddT2.collect());
		System.out.println("Num of Partitions "+rddT.getNumPartitions()+" "+rddT2.getNumPartitions());
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> join=rddT.join(rddR);
		System.out.println(join.collect());
		List<Tuple2<String, Tuple2<Integer, Integer>>> listJoined=join.collect();
		listJoined.forEach(s->System.out.println(((Tuple2<Integer, Integer>)s._2)._2));
	
		
		JavaRDD<Integer> rddInt=jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
		System.out.println("ForeachPartition & Foreach");
		rddInt.foreach(s->System.out.println(s));
		rddInt.foreachPartition(itr->{
			System.out.println("Printing Partition ");
			while (itr.hasNext()) {
			System.out.println(itr.next());	
			}
			
		});
		try(Scanner sc=new Scanner(System.in)) {
			sc.nextLine();
		} finally {
			// TODO: handle finally clause
		}
		
		
		List<Person> inputList = new ArrayList<Person>();
		Person p = new Person();
		p.setFirstName("Arun");
		p.setLastName("Kumar");

		Person p1 = new Person();
		p.setFirstName("Arun");
		p.setLastName("Kumar");

		Person p2 = new Person();
		p.setFirstName("Arun");
		p.setLastName("Kumar");

		inputList.add(p);
		inputList.add(p1);
		inputList.add(p2);

		JavaRDD<Person> personRDD = jsc.parallelize(inputList, 1);
		
		
		List<Student> students = Arrays.asList(new Student("John", Arrays.asList("Math","Math1", "Science")),new Student("Jane", Arrays.asList("English", "History")));
		JavaRDD<Student> studentRDD = jsc.parallelize(students);
		JavaRDD<String> subjectRDD = studentRDD.flatMap(student -> student.getAllSubjects().iterator());
		subjectRDD.foreach(a->System.out.println("flatMap"+a));
		
		JavaRDD<String> subjectRDD1 = studentRDD.map(student -> student.getAllSubjects().toString());
		subjectRDD1.foreach(a->System.out.println("Map"+a));
		
		JavaRDD<String> mathRDD=subjectRDD.filter(a->a.startsWith("Math"));
		mathRDD.foreach(a->System.out.println(a));
		
		JavaPairRDD<String, Integer> inputRDD = jsc.parallelizePairs(Arrays.asList(
	            new Tuple2<>("apple", 3),
	            new Tuple2<>("banana", 2),
	            new Tuple2<>("apple", 5),
	            new Tuple2<>("banana", 4),
	            new Tuple2<>("orange", 6)
	        ));
		inputRDD.countByKey();
		inputRDD.sortByKey();
		try(Scanner sc=new Scanner(System.in)){
			sc.nextLine();
		}
	}
}
