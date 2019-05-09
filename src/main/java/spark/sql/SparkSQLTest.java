package spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLTest {
	
	public static void main(String args[]) {
		System.setProperty("hadoop.home.dir", "D:/Bigdata/hadoop_home/hadoop-common-2.2.0-bin-master");

		SparkSession spark = SparkSession.builder().master("local").getOrCreate();
		Dataset<Row> peopleDS = spark.read().json("data/ml-1m/users.json");
		peopleDS.printSchema();
		System.out.println("number：" + peopleDS.count());
		
		peopleDS.write().csv("data/tmp/1.csv");
		peopleDS.write().json("data/tmp/1.json");
		peopleDS.write().parquet("data/tmp/1.parquet");
		peopleDS.write().orc("data/tmp/1.orc");


		spark.stop();
	}
	
}
