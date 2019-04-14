package com.chelsea.spark.sql;

import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * mysql读取与保存
 * @author shevchenko
 *
 */
public class SparkMysql {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("sqlTest").getOrCreate();
        // 读取mysql
        queryMysql(sparkSession);
        // 保存mysql
        saveMysql(sparkSession);
        sparkSession.close();
    }

    private static void saveMysql(SparkSession sparkSession) {
        Dataset<Row> dataset = sparkSession.read().text(Thread.currentThread().getContextClassLoader().getResource("").toString() + "testTxt");
        JavaRDD<Row> rowRdd = dataset.toJavaRDD();
        JavaRDD<Person> personRdd = rowRdd.map(new Function<Row, Person>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Person call(Row v1) throws Exception {
                String line = String.valueOf(v1.get(0));
                String[] info = line.split(",");
                Person person = new Person();
                person.setId(info[0]);
                person.setName(info[1]);
                person.setAge(Integer.valueOf(info[2]));
                return person;
            }
        });
        Dataset<Row> createDataFrame = sparkSession.createDataFrame(personRdd, Person.class);
        SQLContext sqlContext = sparkSession.sqlContext();
        sqlContext.registerDataFrameAsTable(createDataFrame, "person");
        Dataset<Row> result = sparkSession.sql("select t.age,t.name from person t where age > 15");
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "123456");
        result.write().mode(SaveMode.Append).jdbc("jdbc:mysql://127.0.0.1:3306/test1", "test_user", properties);
    }

    public static void queryMysql(SparkSession sparkSession) {
        SQLContext sqlContext = sparkSession.sqlContext();
        DataFrameReader reader = sqlContext.read().format("jdbc");
        reader.option("url", "jdbc:mysql://127.0.0.1/test1");
        reader.option("driver", "com.mysql.jdbc.Driver");
        reader.option("user", "root");
        reader.option("password", "123456");
        reader.option("dbtable", "users");
        Dataset<Row> load = reader.load();
        sqlContext.registerDataFrameAsTable(load, "u1");
        Dataset<Row> sql = sqlContext.sql("select * from u1");
        sql.show();
    }

}
