package com.chelsea.spark.sql;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * spark解析自定义文件执行sql
 * @author shevchenko
 *
 */
public class SparkSqlByTxtTest {
    
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("sqlTest").getOrCreate();
        // hdfs://172.18.20.237:9000/input/testTxt
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
        List<Row> takeAsList = result.takeAsList(10);
        for (Row row : takeAsList) {
            String name = row.getAs("name");
            Integer age = row.getAs("age");
            System.out.println("name = " + name + ", age = " + age);
        }
        result.foreach(new ForeachFunction<Row>() {
            
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Row row) throws Exception {
                String name = row.getAs("name");
                Integer age = row.getAs("age");
                System.out.println("rdd --------> name = " + name + ", age = " + age);
            }
        });
        sparkSession.close();
    }

}
