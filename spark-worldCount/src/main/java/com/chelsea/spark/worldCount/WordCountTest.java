package com.chelsea.spark.worldCount;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCountTest {
    
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        /**
         * spark运行模式：
         * 1. local -- 在eclipse，IDEA中开发spark程序要用local本地模式，多用于本地测试
         * 2. standalone -- spark自带的资源调度框架，支持分布式搭建，spark任务可以依赖standalone调度资源
         * 3. yarn -- hadoop生态圈中资源调度框架，spark也可以基于yarn调度资源
         * 4. mesos -- 资源调度框架
         */
        // 如果是standalone模式需去掉此行
        conf.setMaster("local");
        conf.setAppName("JavaSparkWordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 读取文件每行数据
        JavaRDD<String> lines = sc.textFile(Thread.currentThread().getContextClassLoader().getResource("").toString() + "wordCount");
        // JavaRDD<String> lines = sc.textFile("hdfs://172.18.20.237:9000/wordcount/input/wordCount");
        // 读取行数据的每个单词
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        // 将单词转换为K,V对
        JavaPairRDD<String, Integer> pairWords = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        // 对每组key对应的value按照自定义逻辑处理，生成新的K,V对
        JavaPairRDD<String, Integer> reduce = pairWords.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        // K,V对进行调换
        JavaPairRDD<Integer, String> mapToPair = reduce.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.swap();
            }
        });
        // 对key进行排序（倒序）
        JavaPairRDD<Integer, String> sortByKey = mapToPair.sortByKey(false);
        // K,V对进行调换
        JavaPairRDD<String, Integer> result = sortByKey.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
                return tuple.swap();
            }
        });
        long count = result.count();
        System.out.println("单词数量：" + count);
        // 遍历新K,V对
        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println("-------------->" + tuple);
            }
        });
        sc.stop();
        sc.close();
    }

}
