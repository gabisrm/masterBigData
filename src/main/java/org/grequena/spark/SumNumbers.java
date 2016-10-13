package org.grequena.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;






public class SumNumbers 
{
    public static void main( String[] args )
    {
    	//primero configuramos spark
        SparkConf conf = new SparkConf();
        
        //declaramos el contexto
        JavaSparkContext context = new JavaSparkContext(conf);
        
        //definimos un array de enteros para hacer la prueba
        Integer[] numbers = new Integer[]{1,2,3,4,5,6,7,8,9};
        
        List<Integer> listOfNumbers = Arrays.asList(numbers);
        
        //creamos un JavaRDD
        JavaRDD<Integer> rddOfNumbers = context.parallelize(listOfNumbers);
        
        /* Con funciones anonimas
        int sum = rddOfNumbers.reduce(new Function2<Integer, Integer, Integer>(){

			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0+arg1;
			}
        	
        });
        */
        
        //hacemos la suma
        //con función lambda
        int sum = rddOfNumbers.reduce((integer1, integer2) -> integer1+integer2);
        
        System.out.println("Suma: "+ sum);
        
        //lo paramos
        context.stop();
        //context.close();
        
        
    }
}
