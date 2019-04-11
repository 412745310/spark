package com.chelsea.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * spark解析json文件执行sql
 * @author shevchenko
 *
 */
public class SparkSqlByJsonTest {
    
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("sqlTest").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().json(Thread.currentThread().getContextClassLoader().getResource("").toString() + "testJson");
        SQLContext sqlContext = sparkSession.sqlContext();
        sqlContext.registerDataFrameAsTable(dataset, "test");
        sparkSession.sql("select t.age,t.name from test t where age > 15").show();
        sparkSession.close();
    }

}
