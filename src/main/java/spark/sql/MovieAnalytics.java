package spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Comparator;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lower;

/*
请利用 RDD API 编写代码片段实现以下功能：

1）请输出该数据集包含的所有不同国家的名称（用到 country 一列，第21列）

2）请输出该数据集中包含的中国电影的数目（用到 country 一列）rdd

3）请输出最受关注的三部中国电影的电影名称、导演以及放映时间
（用到 movie_title第12列 、director_name第2列、num_voted_users第13列、country 以及 title_year第24列 共五列）

请使用 spark sql 的 Dataframe 和 Dataset API 实现以上功能

提示：Dataset<Row> ds = spark.read().option("header","true").csv("movie_metadata.csv");
 */
public class MovieAnalytics {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().master("local").getOrCreate();
		spark.sparkContext().setLogLevel("error");
		/*// 将文件读成RDD
		Dataset<Row> fileDF = spark.read().text("data/spark/movie_metadata.csv");
		Dataset<String> fileDS =
			fileDF.flatMap((FlatMapFunction<Row, String>)r -> Arrays.asList(r.getString(0).split(",")).iterator(),
				Encoders.STRING())*//*.limit(100).foreach((ForeachFunction<String>)w -> System.out.println(w))*//*;
		// 你要实现的代码
		Dataset<String[]> fileTempDS =
			fileDF.map((MapFunction<Row, String[]>)r -> r.getString(0).split(","), Encoders.STRING());
		fileTempDS.map((MapFunction<String[], String>)s -> s[20]).distinct().show();*/
		dobyDataFrame(spark);
		System.out.println("===============");
		doByRDD(spark);

	}

	private static void dobyDataFrame(SparkSession sparkSession) {
		Dataset<Row> df = sparkSession.read().option("header", "true").csv("data/spark/movie_metadata.csv").distinct();
		long amount_country = df.select("country").count();
		System.out.println(amount_country);

		long chinaMovieCount = df.where("lower(country)='china'").count();
		System.out.println(chinaMovieCount);

		df.filter("lower(country)='china'")
			.select("movie_title", "director_name", "num_voted_users", "country", "title_year")
			.orderBy(col("num_voted_users").cast("Integer").desc())
			.limit(3)
			.show(false);

		df.filter(lower(col("country")).equalTo("china"))
			.select("movie_title", "director_name", "num_voted_users", "country", "title_year")
			.orderBy(col("num_voted_users").cast("Integer").desc())
			.limit(3)
			.show(false);

	}

	private static void doByRDD(SparkSession spark) {
		// 将文件读成RDD
		JavaRDD<String> fileRDD =
			spark.sparkContext().textFile("data/spark/movie_metadata.csv", 10).toJavaRDD().distinct();
		//JavaRDD<String[]> arrayRDD = fileRDD.map(line -> line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));

		JavaRDD<String[]> arrayRDD = fileRDD.filter(x -> (!x.startsWith("color,director_name")))
			.map(x -> x.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));

		//请输出该数据集包含的所有不同国家的名称（用到 country 一列，第21列）
		JavaRDD<String> countryRDD = arrayRDD.map(s -> s[20]).distinct();
		long amount_country = countryRDD.count();
		System.out.println(amount_country);

		//请输出该数据集中包含的中国电影的数目（用到 country 一列）rdd
		long chinaMovieCount = arrayRDD.filter(s -> s[20].equalsIgnoreCase("china")).count();
		System.out.println(chinaMovieCount);

		/*
		 * 请输出最受关注的三部中国电影的电影名称、导演以及放映时间
		 * 用到 movie_title第12列 、director_name第2列、num_voted_users第13列、country 以及 title_year第24列 共五列
		 */

		//方式一
		arrayRDD.filter(s -> s[20].equalsIgnoreCase("china"))
			.sortBy(s -> s[12], false, arrayRDD.partitions().size())
			.take(3)
			.forEach(s -> System.out.println(
				"movie_title:" + s[11] + ",director_name:" + s[1] + ",num_voted_users:" + s[12] + ",country:" + s[20]
					+ ",title_year:" + s[23]));

		/*
		movie_title:House of Flying Daggers ,director_name:Yimou Zhang,num_voted_users:92295,country:China,title_year:2004
		movie_title:City of Life and Death ,director_name:Chuan Lu,num_voted_users:8429,country:China,title_year:2009
		movie_title:The Promise ,director_name:Kaige Chen,num_voted_users:8215,country:China,title_year:2005
		 */

		//方式二
		arrayRDD.filter(s -> s[20].equalsIgnoreCase("china"))
			.top(3, new TestComparator())
			.forEach(s -> System.out.println(
				"movie_title:" + s[11] + ",director_name:" + s[1] + ",num_voted_users:" + s[12] + ",country:" + s[20]
					+ ",title_year:" + s[23]));

		/*
		 movie_title:Mission: Impossible - Rogue Nation ,director_name:Christopher McQuarrie,num_voted_users:232187,country:China,title_year:2015
		 movie_title:Hero ,director_name:Yimou Zhang,num_voted_users:149414,country:China,title_year:2002
		 movie_title:House of Flying Daggers ,director_name:Yimou Zhang,num_voted_users:92295,country:China,title_year:2004
		 */
	}

	private static class TestComparator implements Serializable, Comparator<String[]> {
		@Override public int compare(String[] o1, String[] o2) {
			return new Integer(o1[12]).compareTo(new Integer(o2[12]));
		}
	}
}
