package spark.streaming;

import org.apache.hadoop.hbase.client.Put;
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
Program arguments:localhost 9990
 */
public class HBaseStreamingWordCount {
	
	private final static String TABLE_NAME = "streaming_word_count";
	
	private final static String FAMILY_NAME = "w";
	
	public static void main(String[] args)
		throws Exception {
		
		SparkConf sparkConf = new SparkConf().setAppName("HBaseStreamingWordCount");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		
		JavaReceiverInputDStream<String> lines =
			ssc.socketTextStream(args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);
		JavaPairDStream<String, Integer> counts = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
			.mapToPair(w -> new Tuple2<>(w, 1))
			.reduceByKey((v1, v2) -> v1 + v2);
		
		counts.foreachRDD((rdd, time) -> rdd.foreachPartition(recordOfPartitions -> {
			Table table = HbaseTableUtils.createHbaseTable(TABLE_NAME, FAMILY_NAME);
			recordOfPartitions.forEachRemaining(r -> {
				try {
					writeToTable(table, r._1, r._2, time.toString());
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
	
	private static void writeToTable(Table table, String word, Integer count, String time)
		throws Exception {
		Put put = new Put(Bytes.toBytes(time));
		put.addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(word), Bytes.toBytes(count));
		table.put(put);
	}
	
}
