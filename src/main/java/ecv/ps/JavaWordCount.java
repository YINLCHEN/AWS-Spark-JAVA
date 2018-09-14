package ecv.ps;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class JavaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		
		if (args.length < 1) {
			System.err.println("Usage: JavaWordCount <file>" + args);
			System.exit(1);
		}

		SparkSession spark = SparkSession.builder().appName("JavaWordCount").getOrCreate();
		
		//資料來源
		JavaRDD<String> lines = spark.read().textFile("/Users/yinlin.chen/eclipse-workspace/ps/src/main/java/ecv/ps/test.txt").javaRDD(); 
		
		//數據分類
		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

		//依key將相同的值合併
		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

		List<Tuple2<String, Integer>> output = counts.collect();
		
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		
		spark.stop();
	}
}
