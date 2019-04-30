package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class BasicPracticeOne {
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("basicPracticeOne");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		jsc.setLogLevel("WARN");
		List<Integer> result = new ArrayList();
		for (int i = 100; i <= 1000; i++)
			result.add(i);
		JavaRDD<Integer> input = jsc.parallelize(result, 7);
		// TODO add your code here
		System.out.println("head 5 elements are :" + input.take(5));
		System.out.println("sum is :" + input.reduce((a, b) -> a + b));
		System.out.println("avg is :" + input.reduce((a, b) -> a + b) / input.count());

		Tuple2<Integer, Integer> pair = input.mapToPair(i -> new Tuple2<Integer, Integer>(i, 1))
			.reduce((x, y) -> new Tuple2<Integer, Integer>(x._1 + y._1, x._2 + y._2));
		System.out.println("mr avg is :" + pair._1 / pair._2);


		System.out.println("count of even number is :" + input.filter(a -> a % 2 == 0).count());
		System.out.println("head 5 of even number is :" + input.filter(a -> a % 2 == 0).take(5));
		jsc.stop();
		
	}
}
