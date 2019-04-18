package com.chelsea.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

/**
 * spark自定义函数
 * 
 * @author shevchenko
 *
 */
public class SparkUDF {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("sqlTest").getOrCreate();
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
        sparkSession.udf().register("strLen", new MyUDF(), DataTypes.IntegerType);
        Dataset<Row> result = sparkSession.sql("select t.age, strLen(t.name, 10) from person t where age > 15");
        result.show();
        sparkSession.close();
    }
    
    /**
     * 自定义函数实现类
     * @author shevchenko
     *
     */
    static class MyUDF implements UDF2<String, Integer, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer call(String t1, Integer t2) throws Exception {
            return t1.length() + t2;
        }
    }

}
