# JAVA App for Spark job

## Requirements
1. Linux or Ubuntu
2. Java (JVM) (< JAVA 11)
3. Hive Hadoop or Spark
4. (GCP) Data Proc OR ERM (AWS)

## GCP Requirement
1. gCloud Account
2. Enable Data Proc API
3. Install GCP CLI in development Machine

## Sample Code
```java
package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args){
       run_spark();
    }

    /* Spark Session Create with master local with all cluster [*] */
    private static SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName("spark-data-proc-example")
                .master("local[*]")
                .getOrCreate();
    }

    private static void run_spark() {
       SparkSession spark = getSparkSession();
       spark.sparkContext().setLogLevel("WARN");
       /*
        Notes: Change the <APP_ROOT> to your development PC
        Where is application is saved
        For me
         location is "/home/hadoop/java-with-spark"
       */
       String local_file_path = "file:///<APP_ROOT>/data/input/shopping_trends_updated.csv";
       // Load the data from csv file from data folder of the project
       // Create data frame named df by spark.read() function
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load(local_file_path);
        // We use inferSchema true, so Spark will create schema from dataset
        df.printSchema();
        // df.show() will shows the 50 data without trancating the character
        df.show(50, false);
        
        /* We can create the dataset to
           analysis with localtion based and gender based
           Payment method
           df.agg() --> this function will enable to accept 
                        any aggregate function to dataframe, 
                        such as sum, round, avg, min, max
           sum()    --> this function return the sum of the values of specified
                        column from the selected record
           
           round()  --> this function round uo the decimal places 
                        with HALF_UP round mode from selected records
           
           avg()    --> this function using for find the mean value of field
                        or column from the selected rows or records
           
           min()    --> this function will find the minimum value of field
                        or column from the selected rows or records 
           
           max()    --> this function will find the maximum value of field
                        or column from the selected rows or records
           
           count    --> this function count the the number of rows into
                        specified column, if we use grouping, it will 
                        cound the number of rows by specified column
                        with grouping
           
           lit -->  lit is using at spark to convert a literal value into
                    a new column
       */
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
        /* write the result output to csv file after analysis
        repartition -->  this method is used for increase or decrease
                         the number of partition of an RDD or 
                         Dataframe of Spark
         df.write() -->  this method will provide the functionality to
                         writeout the record into file system with
                         specified format
        .mode       -->  we can specify the write ouput forma
                         such as (csv, json...)
        .option     -->  we can spcify the writing file properties
                         like header, schema, ....
        .save       -->  function which is using for save the data into
                         filesystem. 
       */
        String output_file_path="file:///<APP_ROOT>/data/output/location_gender_payment_wise_sales";
        df2.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .option("header", true)
                .save(output_file_path);
    }
}
```

### Build the Application

```bash
> mvn --version
> mvn clean
> mvn install
> mvn package
[INFO] 
[INFO] --- jar:3.3.0:jar (default-jar) @ location_wise_customer ---
[INFO] Building jar: <APP_ROOT>/target/location_wise_customer-1.0-SNAPSHOT.jar
```

### Run into Local Spark Installation
```bash
> export APP_HOME=/home/hadoop/java-with-spark
> spark-submit --class org.example.Main \
 --master local \
 --deploy-mode client \
${APP_HOME}/target/location_wise_customer-1.0-SNAPSHOT.jar \
 --async
```

## Run into Data Proc 

### Create the bucket and upload app
```bash
> > gcloud storage buckets create gs://data_proc_example
> gcloud storage cp target/  \
 gs://data_proc_example/bin --recursive
                                                                                           
```

### run the command to execute
```bash
> export GS_BUCKET=gs://data_proc_example/bin
> export JAR_FILE=location_wise_customer-1.0-SNAPSHOT.jar 
> gcloud dataproc jobs submit spark \
 --region us-east1 \
 --cluster data-proc-example \
 --class org.example.GCPMain \
 --jars ${GS_BUCKET}/${JAR_FILE} \
 --async
```

   
