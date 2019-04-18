package com.chelsea.spark.sql;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * spark自定义聚合函数
 * @author shevchenko
 *
 */
public class SparkUDAF {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        // 设置shuffle分区数，根据数据量合理设置，数据量小，设置值小，数据量大，设置值大
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).master("local").appName("sqlTest").getOrCreate();
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
        sparkSession.udf().register("strCount", new MyUDAF());
        sparkSession.sql("select name, strCount(name) from person group by name").show();
        sparkSession.close();
    }
    
    /**
     * 自定义聚合函数
     * 统计分组内的数据总数
     * @author shevchenko
     *
     */
    static class MyUDAF extends UserDefinedAggregateFunction {

        private static final long serialVersionUID = 1L;
        
        /**
         * 每组数据的初始化值
         */
        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            buffer.update(0, 0);
        }
        
        /**
         * 分区内每组数据的聚合
         * buffer 当前聚合结果
         * row 实际数据
         */
        @Override
        public void update(MutableAggregationBuffer buffer, Row row) {
            System.out.println("当前将要聚合的元素：" + row.get(0).toString());
            buffer.update(0, buffer.getInt(0) + 1);
        }
        
        /**
         * 分区之间每组数据的聚合
         * buffer1 当前聚合结果
         * buffer2 其他分区聚合结果
         */
        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            System.out.println("当前将要聚合的分区结果：" + buffer2.get(0).toString());
            buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0));
        }

        /**
         * 在进行聚合操作的时候所要处理的数据的结果类型
         */
        @Override
        public StructType bufferSchema() {
            return DataTypes.createStructType(Arrays.asList(
                    DataTypes.createStructField("buffer", DataTypes.IntegerType, true)
                    ));
        }

        /**
         * UDAF计算结果的类型
         */
        @Override
        public DataType dataType() {
            return DataTypes.IntegerType;
        }

        /**
         * 用以标记针对给定的一组数据，UDAF是否总是生成相同的结果
         */
        @Override
        public boolean deterministic() {
            return true;
        }

        /**
         * 返回UDAF最后的计算结果，类型要和dataType的类型一致
         */
        @Override
        public Object evaluate(Row buffer) {
            return buffer.getInt(0);
        }

        /**
         * 指定输入字段的数据类型
         */
        @Override
        public StructType inputSchema() {
            return DataTypes.createStructType(Arrays.asList(
                    DataTypes.createStructField("name", DataTypes.StringType, true)
                    ));
        }

    }

}
