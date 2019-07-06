package com.chelsea.spark.core;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 自定义分区器
 * 
 * @author shevchenko
 *
 */
public class PartitionTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("PartitionTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 分区数为3
        JavaPairRDD<String, String> rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("key1", "a"),
                new Tuple2<String, String>("key2", "b"),
                new Tuple2<String, String>("key3", "c"),
                new Tuple2<String, String>("key4", "d"),
                new Tuple2<String, String>("key5", "e"),
                new Tuple2<String, String>("key6", "f")
                ), 3);
        System.out.println("当前分区总数：" + rdd.partitions().size());
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,String>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Iterator<Tuple2<String, String>> t) throws Exception {
                while(t.hasNext()) {
                    Tuple2<String, String> tuple = t.next();
                    System.out.println(tuple._1 + "=" + tuple._2);
                }
            }
        });
        System.out.println("---------重新分区-------");
        // 自定义分区器
        JavaPairRDD<String, String> newRdd = rdd.partitionBy(new Partitioner() {
            
            private static final long serialVersionUID = 1L;

            /**
             * 设置总的分区数
             */
            @Override
            public int numPartitions() {
                return 2;
            }
            
            /**
             * 设置分区规则，每个tuple被分到哪个区
             */
            @Override
            public int getPartition(Object key) {
                int i = Math.abs(String.valueOf(key).hashCode()) % numPartitions();
                if (i == 0) {
                    return 0;
                } else {
                    return 1;
                }
            }
        });
        System.out.println("当前分区总数：" + newRdd.partitions().size());
        newRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,String>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Iterator<Tuple2<String, String>> t) throws Exception {
                while(t.hasNext()) {
                    Tuple2<String, String> tuple = t.next();
                    System.out.println(tuple._1 + "=" + tuple._2);
                }
            }
        });
        sc.close();
    }

}
