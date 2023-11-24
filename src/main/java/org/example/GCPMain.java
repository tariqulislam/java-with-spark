package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class GCPMain {

        public static void main(String[] args){
            run_spark();
        }

        private static SparkSession getSparkSession() {
            return SparkSession.builder()
                    .appName("spark-data-proc-example")
                    .master("yarn")
                    .getOrCreate();
        }
        private static void run_spark() {
            SparkSession spark = getSparkSession();
            spark.sparkContext().setLogLevel("WARN");

            // Load the CSV file from Google Storage Bucket
            Dataset<Row> df = spark.read()
                    .format("csv")
                    .option("header", true)
                    .option("inferSchema", true)
                    .load("gs://data_proc_example/data/input/shopping_trends_updated.csv");
            df.printSchema();
            df.show(50, false);

            Dataset<Row> df2 = df.groupBy("Location","Gender",  "Payment Method")
                    .agg(
                            sum("Purchase Amount (USD)").alias("Total Purchase (USD)"),
                            round(avg("Purchase Amount (USD)"), 2).alias("Average Purchase (USD)"),
                            max("Purchase Amount (USD)").alias("Max Purchase Amount (USD)"),
                            min("Purchase Amount (USD)").alias("Min Purchase Amount (USD)"),
                            count(lit(1)).alias("count")
                    );

            df2.printSchema();
            df2.show(false);
            // location get client information
            df2.repartition(1)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .format("csv")
                    .option("header", true)
                    .save("gs://data_proc_example/data/output/shopping_trends_updated.csv");
        }

}
