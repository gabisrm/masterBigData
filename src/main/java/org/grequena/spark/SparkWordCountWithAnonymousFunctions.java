package org.grequena.spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkWordCountWithAnonymousFunctions {
	static Logger log = Logger
			.getLogger(SparkWordCountWithAnonymousFunctions.class.getName());

	public static void main(String[] args) {

		// Create SparkConfig object
		if (args.length < 1) {
			log.fatal("Syntax Error: there must be one argument (a filename or a directory)");
			throw new RuntimeException();
		}

		SparkConf sparkConf = new SparkConf()
				.setAppName("Spark- wordcount with anonymous functions");

		// create a java spark context
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		// get the text
		JavaRDD<String> lines = sparkContext.textFile(args[0]);

		// create an iterator for all words. We flatmap so that the return value
		// is a single array of all the words in the text
		JavaRDD<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					public Iterator<String> call(String s) {
						return Arrays.asList(s.split(" ")).iterator();
					}
				});

		// for each word in the structure words, we return a pair of the type
		// <word, 1>
		JavaPairRDD<String, Integer> pairs = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});

		// reduce stage. for each key, it iterates and find all the pairs with
		// the same key. The reduce part sums all values with the same key,
		// returning a pair of <key,totalValue>
		JavaPairRDD<String, Integer> counts = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				});
		
		JavaPairRDD<String,Integer> sortedPairs= counts.sortByKey();
		
		List<Tuple2<String,Integer>> output = sortedPairs.collect();
		
		for(Tuple2<?,?> tuple : output){
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		
		sparkContext.stop();
	}
}
