package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple4;

public class BasicPracticeFour {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("basicPracticeFour");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		// TODO add your code here
		String FILE_PATH = "data/WalmartFile";
		JavaRDD<String> fileRdd = jsc.textFile(FILE_PATH);
		
		/*
		 * id, sum(x), sum(y), sum(z)
		 */
		JavaPairRDD<String, Tuple4<Integer, Integer, Integer, Integer>> storedRdd = fileRdd.map(x -> x.split(","))
			.mapToPair(x -> new Tuple2<>(x[0], new Tuple4<>(Integer.parseInt(x[1]), Integer.parseInt(x[2]), Integer.parseInt(x[3]), 1)));
		
		storedRdd.reduceByKey((t1, t2) -> new Tuple4<>(t1._1() + t2._1(), t1._2() + t2._2(), t1._3() + t2._3(), t1._4() + t2._4()))
			.foreach(s -> System.out
				.println(s._1 + ": sum(x)=" + s._2._1() + ",sum(y)=" + s._2._2() + ",sum(z)=" + s._2._3() + ", count=" + s._2._4()));
		
		/*
		 * id, max(y), min(z), avg(x)
		 */
		JavaPairRDD<String, Tuple4<Integer, Integer, Integer, Integer>> pairRDD = storedRdd.reduceByKey((t1, t2) -> {
			int sum_x = t1._1() + t2._1();
			int count = t1._4() + t2._4();
			int max_y = Math.max(t1._2(), t2._2());
			int min_z = Math.min(t1._3(), t2._3());
			return new Tuple4<>(sum_x, max_y, min_z, count);
		});
		
		pairRDD
			.foreach(s -> System.out.println(s._1 + ":max(y)=" + s._2._2() + ",min(z)=" + s._2._3() + ", avg(x)=" + s._2._1() / s._2._4()));
		
		jsc.stop();
	}
}
