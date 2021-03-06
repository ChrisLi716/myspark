package spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/*

run in local

VM options:-Dspark.master=local[2]
Program arguments:localhost 9990


run in Yarn

bin/spark-submit \
  --master yarn-cluster \
  --class spark.streaming.StreamingWordCount \
  --name "streaming word count"\
  --driver-memory 512m \
  --executor-memory 512m \
  --executor-cores 1 \
  --num-executors 2 \
 demojars/myspark.jar \
 localhost 9990
 */
public class StreamingWordCount {
	
	public static void main(String[] args)
		throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: StreamingWordCount <hostname> <port>");
			System.exit(1);
		}
		
		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("StreamingWordCount ");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
		
		JavaReceiverInputDStream<String> lines =
			ssc.socketTextStream(args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);
		
		/*JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);*/
		
		// window操作
		JavaDStream<String> window_words =
			lines.window(Durations.seconds(10), Durations.seconds(5)).flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		JavaPairDStream<String, Integer> wordCounts = window_words.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);
		wordCounts.print();
		ssc.start();
		ssc.awaitTermination();
		
	}
}
