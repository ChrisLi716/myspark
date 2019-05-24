package spark.streaming;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/*
run in local

VM options:-Dspark.master=local[2]
Program arguments:localhost 9992

scan data in hbase
scan 'streaming_word_count_incr', {COLUMNS => 'w:c:toLong'}
 */
public class HBaseStreamingWordCountIncr {
	
	private final static String TABLE_NAME = "streaming_word_count_incr";
	
	private final static String FAMILY_NAME = "w";
	
	private final static String QUALIFY = "c";
	
	public static void main(String[] args)
		throws Exception {
		
		SparkConf sparkConf = new SparkConf().setAppName("HBaseStreamingWordCountIncr");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(20));
		
		JavaReceiverInputDStream<String> lines =
			ssc.socketTextStream(args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);
		JavaPairDStream<String, Integer> counts = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
			.mapToPair(w -> new Tuple2<>(w, 1))
			.reduceByKey((v1, v2) -> v1 + v2);
		
		counts.foreachRDD((rdd, time) -> rdd.foreachPartition(recordOfPartitions -> {
			Table table = HbaseTableUtils.createHbaseTable(TABLE_NAME, FAMILY_NAME);
			recordOfPartitions.forEachRemaining(r -> {
				try {
					writeToTable(table, r._1, r._2);
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			});
			table.close();
		}));
		
		ssc.start();
		ssc.awaitTermination();
		
	}
	
	private static void writeToTable(Table table, String word, Integer count)
		throws Exception {
		table.incrementColumnValue(Bytes.toBytes(word), Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(QUALIFY), count);
	}
	
}
