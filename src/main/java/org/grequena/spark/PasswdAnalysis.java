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

public class PasswdAnalysis {
	static Logger log = Logger.getLogger(PasswdAnalysis.class.getName());

	public static void main(String[] args) {

		// Create SparkConfig object
		if (args.length < 1) {
			log.fatal("Syntax Error: there must be one argument (a filename or a directory)");
			throw new RuntimeException();
		}

		SparkConf sparkConf = new SparkConf()
				.setAppName("Spark- passwdAnalysis task");

		// create a java spark context
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		// get the text. We cache it so we can reuse the JavaRDD structure for
		// the 3 processing requirements
		JavaRDD<String> lines = sparkContext.textFile(args[0]).cache();

		// we print out the number of users
		long numberOfUsers = lines.count();

		// we sort the users by username and collect only the first 5
		List<String> firstFive = lines.takeOrdered(5);

		// and we get the number of users which has the command /bin/bash as
		// last field
		// we filter all lines so that we only count those users which the last
		// field (userLine.split(":")[6], there are 7 fields) equals to the string
		// "/bin/bash
		long binBashUsers = lines.filter(
				(userLine) -> userLine.split(":")[6]
						.equals("/bin/bash")).count();

		// we print number of users
		System.out.println("The number of user accounts is: " + numberOfUsers);
		// we print the first 5 usernames, only with the fields user:UID:GID
		for (String userLine : firstFive) {
			// we split the fields
			String[] fields = userLine.split(":");

			// print out the corresponding fields
			if (fields.length > 3) {
				System.out.println(fields[0] + ":" + fields[2] + ":"
						+ fields[3]);
			}
		}

		// we print the number of users wich has /bin/bash as last field
		System.out
				.println("The number of users having bash as command when logging is: "
						+ binBashUsers);

		sparkContext.stop();
	}
}
