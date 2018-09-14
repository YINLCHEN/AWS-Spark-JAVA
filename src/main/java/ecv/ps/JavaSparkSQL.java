package ecv.ps;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class JavaSparkSQL {

	/*
	public class Record implements Serializable {
		String department;
		String designation;
	}
	*/

	public static void main(String[] args) {

		if (args.length < 1) {
			System.err.println("Usage: JavaSparkSQL <file>");
			System.exit(1);
		}

		SparkSession spark = SparkSession.builder().appName("JavaSparkSQL").getOrCreate();

		Dataset<Row> csv = spark.read().format("csv").option("header", "true")
				.load("s3a://AKIAIEZADZZKNVVV5BZQ:azfX3%2FbBPtlxHb1QuVEuI%2FYhamx9tYTvo0WfGGVe@emr-yinlchen/yellow_tripdata_2017-01_thin.csv");
		///Users/yinlin.chen/Desktop/yellow_tripdata_2017-01.csv

		csv.createOrReplaceTempView("Data");

		csv = spark.sql(
				  "SELECT tpep_pickup_datetime, ROUND(SUM(total_amount), 0) total_amount "
				+ "FROM Data "
				+ "GROUP BY tpep_pickup_datetime");

		csv.show();
		
		csv.repartition(1)
		.write()
		.mode(SaveMode.Overwrite)
		.format("com.databricks.spark.csv")
		.option("header", true)
		.save("s3a://AKIAIEZADZZKNVVV5BZQ:azfX3%2FbBPtlxHb1QuVEuI%2FYhamx9tYTvo0WfGGVe@emr-yinlchen/JavaSparkSQLOutput_0913/");
		
		spark.stop();
	}
}
