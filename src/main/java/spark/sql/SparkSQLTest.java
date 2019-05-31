package spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLTest {

	public static void main(String args[]) {
		// System.setProperty("hadoop.home.dir", "D:/Bigdata/hadoop_home/hadoop-common-2.2.0-bin-master");

		SparkSession spark = SparkSession.builder().master("local").appName("read and write datasource").getOrCreate();
		Dataset<Row> peopleDF = spark.read().json("data/ml-1m/users.json");
		peopleDF.printSchema();
		System.out.println("number：" + peopleDF.count());

		peopleDF.write().csv("data/ml-1m/output/1.csv");
		peopleDF.write().json("data/ml-1m/output/1.json");
		peopleDF.write().parquet("data/ml-1m/output/1.parquet");
		peopleDF.write().orc("data/ml-1m/output/users.orc");

		Dataset<Row> ratingsDF = spark.read().format("text").load("data/ml-1m/ratings.dat");
		ratingsDF.toJSON().write().json("data/ml-1m/output/ratings.json");

		spark.stop();
	}

}
