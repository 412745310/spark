package com.chelsea.spark.core;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * 转换和触发类算子
 * 返回值为RDD类型的是转换类算子，否则为触发类算子
 * 转换类算子是懒加载，必须有触发类算子才会执行
 * 
 * @author shevchenko
 *
 */
public class TransformationAndActionTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("JavaSparkWordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(Thread.currentThread().getContextClassLoader().getResource("").toString() + "wordCount");
        // 读取行数据的每个单词
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        // 随机抽样，true代表是否放回抽样, 0.5代表抽样比例
        JavaRDD<String> rdd = words.sample(true, 0.5);
        // 获取总数
        System.out.println(rdd.count());
        // 获取列表第一条记录
        System.out.println(rdd.first());
        // 获取列表所有记录
        System.out.println(rdd.collect());
        // 获取列表前5条记录
        System.out.println(rdd.take(5));
        
        sc.close();
    }
    
}
