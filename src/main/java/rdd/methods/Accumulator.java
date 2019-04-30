package rdd.methods;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

public class Accumulator {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("Accumulator");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		LongAccumulator accum = jsc.sc().longAccumulator();
		jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5)).foreach(x -> accum.add(1));
		System.out.println(accum.value());
	}
	
}
