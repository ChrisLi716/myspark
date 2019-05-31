package spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Join {

	public static void main(String[] args) {

		// 需要提前交users.dat和ratings.dat转化为json格式，并命名成users.json和ratings.json
		SparkSession spark = SparkSession.builder().master("local").appName("join").getOrCreate();
		Dataset<Row> userDF = spark.read().load("/tmp/users.json");
		Dataset<Row> ratingDF = spark.read().load("/tmp/ratings.json");

		ratingDF.filter("movieID = 2116")
			.join(userDF, "userID")
			.select("gender", "age")
			.groupBy("gender", "age")
			.count()
			.show();

	}
}
