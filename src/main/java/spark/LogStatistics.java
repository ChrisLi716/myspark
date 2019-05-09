package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

public class LogStatistics {
	
	/**
	 * @param args args[0] = data\access.log
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("LogStatistics");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		LongAccumulator total = jsc.sc().longAccumulator();
		LongAccumulator count400 = jsc.sc().longAccumulator();
		LongAccumulator count200 = jsc.sc().longAccumulator();

		JavaRDD<String> input = jsc.textFile(args[0]);
		input.foreach(s -> {
			String[] row = s.split(",");
			// TODO add your code here
			total.add(1);
			
			if (row[0].equals("200")) {
				count200.add(1);
			}
			else if (row[0].equals("400")) {
				count400.add(1);
			}
			
		});
		// TODO add your code here
		System.out.println("total:" + total.value());
		System.out.println("count200:" + count200.value());
		System.out.println("count400:" + count400.value());
		jsc.stop();
		
	}
}
