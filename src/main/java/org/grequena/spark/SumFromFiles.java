package org.grequena.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class SumFromFiles {
	 public static void main( String[] args )
	    {
	    	//primero configuramos spark
	        SparkConf conf = new SparkConf();
	        
	        //declaramos el contexto
	        JavaSparkContext context = new JavaSparkContext(conf);
	        
	        //args[0] puede ser un archivo o un directorio lleno de archivos de texto. Lo que hace es obtener cada LINEA de cada fichero y meterlo en una lista JavaRDD
	        JavaRDD<String> lines = context.textFile(args[0]);
	        
	        //mapeamos para obtener los valores en integer
	        JavaRDD<Integer> rddOfIntegers = lines.map(s -> Integer.valueOf(s));
	        
	        //reduzco para obtener la suma
	        int sum = rddOfIntegers.reduce((integer1, integer2) -> integer1+integer2);
	        
	        System.out.println("Suma: "+ sum);
	        
	        //lo paramos
	        context.stop();
	        //context.close();
	        
	        
	    }
}
