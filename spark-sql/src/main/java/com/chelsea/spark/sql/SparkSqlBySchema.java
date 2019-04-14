package com.chelsea.spark.sql;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 动态创建schema方式加载Dataset
 * 
 * @author shevchenko
 *
 */
public class SparkSqlBySchema {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("sqlTest").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().text(Thread.currentThread().getContextClassLoader().getResource("").toString() + "testTxt");
        JavaRDD<Row> rowRdd = dataset.toJavaRDD();
        JavaRDD<Row> mapRow = rowRdd.map(new Function<Row, Row>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Row call(Row row) throws Exception {
                String line = String.valueOf(row.get(0));
                String[] info = line.split(",");
                Row newRow = RowFactory.create(info[0], info[1], Integer.valueOf(info[2]));
                return newRow;
            }
        });
        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true)
        );
        StructType schema = DataTypes.createStructType(fields);
        SQLContext sqlContext = sparkSession.sqlContext();
        Dataset<Row> result = sqlContext.createDataFrame(mapRow, schema);
        result.foreach(new ForeachFunction<Row>() {
            
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Row row) throws Exception {
                String id = row.getAs("id");
                String name = row.getAs("name");
                Integer age = row.getAs("age");
                System.out.println("id = " +id+ ", name = " + name + ", age = " + age);
            }
        });
        sparkSession.close();
    }

}
