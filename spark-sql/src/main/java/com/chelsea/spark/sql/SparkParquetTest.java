package com.chelsea.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * 读写parquet文件
 * 
 * @author shevchenko
 *
 */
public class SparkParquetTest {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("sqlTest").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().text(Thread.currentThread().getContextClassLoader().getResource("").toString() + "testTxt");
        dataset.write().mode(SaveMode.Overwrite).format("parquet").save("C:/Users/Administrator/Desktop/parquet");
        SQLContext sqlContext = sparkSession.sqlContext();
        Dataset<Row> load = sqlContext.read().format("parquet").load("C:/Users/Administrator/Desktop/parquet");
        load.show();
        sparkSession.close();
    }

}
