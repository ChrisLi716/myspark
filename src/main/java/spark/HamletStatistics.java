package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HamletStatistics {
	
	/**
	 * • 将读取的停止词广播到各个executor • 使用累加器同时统计总单词数和总停止词数 • 输出出现次数最高的前10个单词
	 * 
	 * @param args data\textfile\stopword.txt data\textfile\Hamlet.txt
	 */
	public static void main(String[] args) {
		
		// System.setProperty("hadoop.home.dir", "D:\\Bigdata\\hadoop_home\\hadoop-common-2.2.0-bin-master");
		
		SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("HamletStatistics");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		LongAccumulator stopWordsAccumulator = jsc.sc().longAccumulator();
		LongAccumulator validWordsAccumulator = jsc.sc().longAccumulator();
		
		JavaRDD<String> stopwords = jsc.textFile(args[0]);
		// TODO 1.collect stopwords and broadcast to executors
		Set<String> stopwordsSet = new HashSet<>(stopwords.collect());
		Broadcast<Set<String>> stopwordsBroadcast = jsc.broadcast(stopwordsSet);
		
		// TODO 2.define accumulators, countTotal and stopTotal
		JavaRDD<String> input = jsc.textFile(args[1]);
		JavaRDD<String> words = input.flatMap(s -> splitWords(s).iterator());
		JavaRDD<String> filteredWords = words.filter(w -> {
			if (stopwordsBroadcast.getValue().contains(w)) {
				stopWordsAccumulator.add(1);
				return false;
			}
			else {
				validWordsAccumulator.add(1);
				return true;
			}
		});
		
		JavaPairRDD<String, Integer> counts = filteredWords.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((x, y) -> x + y);
		// TODO sort result
		JavaPairRDD<String, Integer> resultRdd =
			counts.mapToPair(s -> new Tuple2<>(s._2, s._1)).sortByKey(false).mapToPair(s -> new Tuple2<>(s._2, s._1));
		// TODO output result
		System.out.println(resultRdd.take(10));
		
		System.out.println("stopWordsAccumulator:" + stopWordsAccumulator.value());
		System.out.println("validWordsAccumulator:" + validWordsAccumulator.value());
		
		jsc.stop();
	}
	
	public static List<String> splitWords(String line) {
		List<String> result = new ArrayList<String>();
		String[] words = line.replaceAll("['.,:?!-]", "").split("\\s");
		for (String w : words) {
			if (!w.trim().isEmpty()) {
				result.add(w.trim());
			}
		}
		return result;
	}
	
}
