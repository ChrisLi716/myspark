package spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class StreamingWordCount {
	
	/**
	 * VM options:-Dspark.master=local[2]
	 *
	 * @param args Program arguments:localhost 9990
	 * @throws Exception
	 */
	public static void main(String[] args)
		throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: StreamingWordCount <hostname> <port>");
			System.exit(1);
		}
		
		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("StreamingWordCount ");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		
		JavaReceiverInputDStream<String> lines =
			ssc.socketTextStream(args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);
		JavaDStream<String> words = lines.flatMap((FlatMapFunction<String, String>)x -> Arrays.asList(x.split(" ")).iterator());
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair((PairFunction<String, String, Integer>)s -> new Tuple2<>(s, 1))
			.reduceByKey((Function2<Integer, Integer, Integer>)(i1, i2) -> i1 + i2);
		
		wordCounts.print();
		ssc.start();
		ssc.awaitTermination();
		
	}
}
