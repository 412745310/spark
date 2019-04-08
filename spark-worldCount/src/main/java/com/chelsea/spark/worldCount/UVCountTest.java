package com.chelsea.spark.worldCount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * UV统计降序排列
 * 
 * @author shevchenko
 *
 */
public class UVCountTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("UVCountTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 读取文件
        JavaRDD<String> lines = sc.textFile(Thread.currentThread().getContextClassLoader().getResource("").toString() + "pvuvdata");
        // 行数据转换为k,v对
        JavaPairRDD<String, Integer> mapToPair = lines.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                String[] words = line.split("\\t");
                String url = words[5];
                return new Tuple2<String, Integer>(url, 1);
            }});
        // 自定义key聚合逻辑，生成新的k,v对
        JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        // k,v进行调换
        JavaPairRDD<Integer, String> mapToPair2 = reduceByKey.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.swap();
            }
        });
        // 按照key降序排序
        JavaPairRDD<Integer, String> sortByKey = mapToPair2.sortByKey(false);
        // k,v进行调换
        JavaPairRDD<String, Integer> mapToPair3 = sortByKey.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
                return tuple.swap();
            }
        });
        // 遍历新k,v结果
        mapToPair3.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple._1 + "=" + tuple._2);
            }
        });
        sc.close();
    }

}
