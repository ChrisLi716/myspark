package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
	
	/**
	 * set program arguments if run in local idea
	 * D:\IdeaWorksapce\myspark\src\main\java\spark\words D:\IdeaWorksapce\myspark\output
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("wordCount");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> lines = jsc.textFile(args[0]);
		lines.cache();
		JavaRDD<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		JavaPairRDD<String, Integer> counts = words.mapToPair(w -> new Tuple2<>(w, 1)).reduceByKey((x, y) -> x + y);
		counts.saveAsTextFile(args[1]);
		System.out.println(counts.collect());
		jsc.stop();
	}
}
