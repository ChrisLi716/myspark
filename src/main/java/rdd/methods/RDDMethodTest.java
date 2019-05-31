package rdd.methods;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RDDMethodTest {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("RDDMethodTest");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		sortByTest(jsc);
	}

	private static void mapTest(JavaSparkContext jsc) {
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(data, 3);
		rdd.map(i -> i + 1).foreach(e -> System.out.println(e));
	}

	private static void flatMapTest(JavaSparkContext jsc) {
		List<List<Integer>> list = Arrays.asList(Arrays.asList(1),
			Arrays.asList(2, 3),
			Arrays.asList(4),
			Arrays.asList(5, 6),
			Arrays.asList(7),
			Arrays.asList(8));
		JavaRDD<List<Integer>> rdd = jsc.parallelize(list, 3);
		rdd.flatMap(List::iterator)/* .map(i -> i + 100) */.foreach(e -> System.out.println(e));

		List<List<String>> strList =
			Arrays.asList(Arrays.asList("Hello world"), Arrays.asList("Chris", "China"), Arrays.asList("31", "man"));
		JavaRDD<List<String>> strRDD = jsc.parallelize(strList, 6);
		System.out.println(strRDD.flatMap(List::iterator)
			.map(e -> e.split(" "))
			.map(e -> Arrays.asList(e))
			.flatMap(e -> e.iterator())
			.collect());

	}

	/**
	 * coalesce(numPartitions: Int) : RDD[T] 将RDD中的partition个数合并为numPartitions个
	 * <p>
	 * coalesce() 可以将 parent RDD 的 partition 个数进行调整，比如从 5 个减少到 3 个，或者从 5 个增加到 10 个。需要注意的是当 shuffle = false 的时候，是不能增加
	 * partition 个数的（即不能从 5 个变为 10 个）。
	 *
	 * @param jsc
	 */
	private static void coalesceTest(JavaSparkContext jsc) {
		List<String> list = Arrays.asList("Hello", "I'm Chris", "welcome to my world!");
		JavaRDD<String> rdd = jsc.parallelize(list, 4);
		System.out.println(rdd.glom().collect());
		System.out.println(rdd.coalesce(2).glom().collect());
		System.out.println(rdd.coalesce(6, true).glom().collect());
	}

	/**
	 * 将RDD中的 partition个数均匀合并为numPartitions个
	 * <p>
	 * 如果使用repartition对RDD的partition数目进行缩减操作，可以使用coalesce函数，将shuffle设置为false，避免shuffle过程，提高效率。
	 *
	 * @param jsc
	 */
	private static void repartitionTest(JavaSparkContext jsc) {
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);
		JavaRDD<Integer> rdd = jsc.parallelize(list, 4);
		System.out.println(rdd.glom().collect());
		System.out.println(rdd.repartition(5).glom().collect());
		System.out.println(rdd.repartition(2).glom().collect());
	}

	/**
	 * 将RDD中每个partition中元素转换为数组
	 *
	 * @param jsc
	 */
	private static void glomTest(JavaSparkContext jsc) {
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);
		JavaRDD<Integer> rdd = jsc.parallelize(list, 8);
		System.out.println(rdd.glom().collect());
	}

	/**
	 * 将RDD中每个partition中元素转换为数组
	 *
	 * @param jsc
	 */
	private static void takeTest(JavaSparkContext jsc) {
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);
		JavaRDD<Integer> rdd = jsc.parallelize(list, 3);
		System.out.println(rdd.take(3));
		System.out.println(rdd.glom().take(3));
		System.out.println(rdd.first());
	}

	/**
	 * 将RDD中每个partition中元素转换为数组
	 *
	 * @param jsc
	 */
	private static void topTest(JavaSparkContext jsc) {
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);
		JavaRDD<Integer> rdd = jsc.parallelize(list, 7);
		System.out.println(rdd.top(3));
	}

	private static void mapValuesTest(JavaSparkContext jsc) {
		List<Tuple2<String, Integer>> pairs =
			Arrays.asList(new Tuple2<>("A", 0), new Tuple2<>("B", 2), new Tuple2<>("A", 2), new Tuple2<>("C", 4));
		JavaPairRDD<String, Integer> rdd = jsc.parallelizePairs(pairs, 3);
		System.out.println(rdd.mapValues(e -> e + 1).collect());
		System.out.println(rdd.reduceByKey((m, n) -> m + n).collect());
	}

	private static void reduceByKeyTest(JavaSparkContext jsc) {
		List<Tuple2<String, Integer>> pairs = Arrays.asList(new Tuple2<>("A", 0),
			new Tuple2<>("B", 2),
			new Tuple2<>("A", 2),
			new Tuple2<>("C", 4),
			new Tuple2<>("A", 100),
			new Tuple2<>("B", 200));
		JavaPairRDD<String, Integer> rdd = jsc.parallelizePairs(pairs, 3);
		System.out.println(rdd.reduceByKey((m, n) -> m + n).collect());
	}

	private static void groupBykeyTest(JavaSparkContext jsc) {
		List<Tuple2<String, Integer>> pairs = Arrays.asList(new Tuple2<>("A", 0),
			new Tuple2<>("B", 2),
			new Tuple2<>("A", 2),
			new Tuple2<>("C", 4),
			new Tuple2<>("A", 100),
			new Tuple2<>("B", 200));
		JavaPairRDD<String, Integer> rdd = jsc.parallelizePairs(pairs, 3);
		System.out.println(rdd.groupByKey().collect());
	}

	private static void unionTest(JavaSparkContext jsc) {
		List<Tuple2<String, Integer>> pairs1 =
			Arrays.asList(new Tuple2<>("A", 0), new Tuple2<>("B", 2), new Tuple2<>("A", 3), new Tuple2<>("C", 4));
		JavaPairRDD<String, Integer> rdd1 = jsc.parallelizePairs(pairs1, 3);

		List<Tuple2<String, Integer>> pairs2 = Arrays.asList(new Tuple2<>("A", 100), new Tuple2<>("B", 200));
		JavaPairRDD<String, Integer> rdd2 = jsc.parallelizePairs(pairs2, 3);
		System.out.println(rdd1.union(rdd2).collect());
	}

	private static void joinTest(JavaSparkContext jsc) {
		List<Tuple2<String, Integer>> pairs1 =
			Arrays.asList(new Tuple2<>("A", 0), new Tuple2<>("B", 2), new Tuple2<>("A", 3), new Tuple2<>("C", 4));
		JavaPairRDD<String, Integer> rdd1 = jsc.parallelizePairs(pairs1, 3);

		List<Tuple2<String, Integer>> pairs2 = Arrays.asList(new Tuple2<>("A", 100), new Tuple2<>("B", 200));
		JavaPairRDD<String, Integer> rdd2 = jsc.parallelizePairs(pairs2, 3);

		JavaPairRDD<String, Tuple2<Integer, Integer>> joinPairedRdd1 = rdd1.join(rdd2);
		System.out.println(joinPairedRdd1.collect());

		List<Tuple2<String, Integer>> pairs3 = Arrays.asList(new Tuple2<>("A", 1000), new Tuple2<>("B", 2000));
		JavaPairRDD<String, Integer> rdd3 = jsc.parallelizePairs(pairs3, 3);
		JavaPairRDD<String, Tuple2<Tuple2<Integer, Integer>, Integer>> johnPairedRdd2 = joinPairedRdd1.join(rdd3);
		System.out.println(johnPairedRdd2.collect());
	}

	private static void mapToPairTest(JavaSparkContext jsc) {
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);
		JavaRDD<Integer> rdd1 = jsc.parallelize(list, 3);
		JavaRDD<Tuple2> rdd2 = rdd1.map(e -> new Tuple2<>(e, e - 1));
		JavaPairRDD<Integer, Integer> rdd3 = rdd1.mapToPair(e -> new Tuple2<>(e, e + 1));
		System.out.println(rdd2.collect());
		System.out.println(rdd3.collect());
	}

	/**
	 * zip的两个RDD的partition必须相同，不然会报出如下错误 java.lang.IllegalArgumentException: Can't zip RDDs with unequal numbers of
	 * partitions: List(3, 6)
	 *
	 * @param jsc
	 */
	private static void zipTest(JavaSparkContext jsc) {
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);
		List<String> listA = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "G");
		JavaRDD<Integer> rdd1 = jsc.parallelize(list, 3);
		JavaRDD<String> rdd2 = jsc.parallelize(listA, 3);
		JavaPairRDD<Integer, String> rdd3 = rdd1.zip(rdd2);
		System.out.println(rdd3.collect());
	}

	private static void sortByTest(JavaSparkContext jsc) {
		List<Tuple2<String, Integer>> pairs1 =
			Arrays.asList(new Tuple2<>("A", 0), new Tuple2<>("B", 2), new Tuple2<>("A", 3), new Tuple2<>("C", 4));
		JavaRDD<Tuple2<String, Integer>> javaRDD = jsc.parallelize(pairs1, 3);
		javaRDD.sortBy(s -> s._1, false, javaRDD.partitions().size()).foreach(s -> System.out.println(s.toString()));
	}
}
