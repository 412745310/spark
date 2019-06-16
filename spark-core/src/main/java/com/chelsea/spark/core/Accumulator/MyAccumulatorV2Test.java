package com.chelsea.spark.core.Accumulator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * 自定义累加器AccumulatorV2
 * 
 * @author shevchenko
 *
 */
public class MyAccumulatorV2Test {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("MyAccumulatorV2Test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("A", "B", "A", "D", "E", "D", "G", "H", "I", "A", "B", "I", "G", "D", "I");
        // 有3个分区，累加器就执行3次merge方法
        JavaRDD<String> cacheRdd = sc.parallelize(list, 3).cache();
        MyAccumulatorV2 myAccumulatorV2 = new MyAccumulatorV2();
        // 需要注册，不然在运行过程中，会抛出一个序列化异常
        sc.sc().register(myAccumulatorV2);
        cacheRdd.foreach(new VoidFunction<String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(String t) throws Exception {
                myAccumulatorV2.add(t);
            }

        });
        System.out.println(myAccumulatorV2.value());
        sc.close();
    }

}
