package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class BasicPracticeThree {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("basicPracticeThree");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		jsc.setLogLevel("error");
		List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2("coffee",
			1), new Tuple2("coffee", 3), new Tuple2("panda", 4), new Tuple2("coffee", 5), new Tuple2("street", 2), new Tuple2("panda", 5));
		JavaPairRDD<String, Integer> input = jsc.parallelizePairs(data);
		
		// [(coffee,(1,1)), (coffee,(3,1)), (panda,(4,1)), (coffee,(5,1)), (street,(2,1)), (panda,(5,1))]
		System.out.println(input.mapValues(value -> new Tuple2<Integer, Integer>(value, 1)).collect());
		
		// [(coffee,(9,3)), (street,(2,1)), (panda,(9,2))]
		System.out.println(input.mapValues(value -> new Tuple2<Integer, Integer>(value, 1))
			.reduceByKey((tuple1, tuple2) -> new Tuple2<Integer, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2))
			.collect());
		
		// TODO add your code here
		JavaPairRDD<String, Double> averagePair = input.mapValues(value -> new Tuple2<Integer, Integer>(value, 1)) // <String, Int, Int>
			.reduceByKey((tuple1, tuple2) -> new Tuple2<Integer, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2))
			.mapToPair(x -> new Tuple2<String, Double>(x._1, x._2._1 * 1.0 / x._2._2));

		// [(coffee,3.0), (street,2.0), (panda,4.5)]
		System.out.println("output : \n" + averagePair.collect());
		jsc.stop();
	}
}
