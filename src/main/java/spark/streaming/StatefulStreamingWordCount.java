package spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import scala.Option;
import scala.Tuple2;

import java.util.Arrays;

/*
checkpoint程序必须运行在hdfs环境上

run in Yarn

 bin/spark-submit \
  --master yarn-client \
  --class spark.streaming.StatefulStreamingWordCount \
  --name "Stateful Streaming WordCount" \
  --driver-memory 512m \
  --executor-memory 512m \
  --executor-cores 1 \
  --num-executors 2 \
 demojars/myspark.jar \
 /tmp/sparkstreaming/checkpoint hadoop 9991


 */
public class StatefulStreamingWordCount {
	
	public static void main(String[] args)
		throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: StatefulStreamingWordCount <check_dir> <hostname> <port>");
			System.exit(1);
		}
		String checkPointDir = args[0];
		String host = args[1];
		int port = Integer.parseInt(args[2]);
		// Create the context with a 1 second batch size
		JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkPointDir, () -> createContext(checkPointDir, host, port));
		ssc.start();
		// ssc.remember(Durations.minutes(1));
		ssc.awaitTermination();
		
	}
	
	public static JavaStreamingContext createContext(String checkPointDir, String host, int port) {
		SparkConf sparkConf = new SparkConf().setAppName("StatefulStreamingWordCount");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		ssc.checkpoint(checkPointDir);
		ssc.sparkContext().setLogLevel("WARN");
		
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, port, StorageLevels.MEMORY_AND_DISK_SER_2);
		JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		JavaPairDStream<String, Integer> wordPairs = words.mapToPair(w -> new Tuple2<>(w, 1));

		wordPairs.cache();
		
		JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateWordCounts =
			wordPairs.mapWithState(StateSpec.function(new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> call(String word, Optional<Integer> one, State<Integer> state) {
					Option<Integer> stateCount = state.getOption();
					Integer sum = one.orElse(0);
					if (stateCount.isDefined()) {
						sum += stateCount.get();
					}
					state.update(sum);
					return new Tuple2<>(word, sum);
				}
			}));
		
		stateWordCounts.print();
		stateWordCounts.stateSnapshots().print();
		
		return ssc;
	}
	
}
