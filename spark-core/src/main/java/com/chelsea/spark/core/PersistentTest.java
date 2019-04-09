package com.chelsea.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 持久化类算子cache persist checkpoint
 * cache persist是懒加载，需要action类算子触发
 * cache只持久化到内存
 * persist既可以持久化到内存，也可以持久化到磁盘
 * 
 * @author shevchenko
 *
 */
public class PersistentTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("JavaSparkWordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setCheckpointDir("C:/Users/Administrator/Desktop/checkpoint");
        JavaRDD<String> lines = sc.textFile(Thread.currentThread().getContextClassLoader().getResource("").toString() + "wordCount");
        // 将rdd持久化到内存中
        // lines.cache();
        // 将rdd持久化到内存和磁盘中
        // lines.persist(StorageLevel.MEMORY_AND_DISK());
        // 将rdd标记为检查点，并将rdd保存到指定文件中
        lines.checkpoint();
        long startTime = System.currentTimeMillis();
        // 第一次从磁盘中加载数据并计算
        long count1 = lines.count();
        System.out.println("count1 :" + count1 + ", time : " + (System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();
        // 第二次从内存中直接获取数据并计算
        long count2 = lines.count();
        System.out.println("count2 :" + count2 + ", time : " + (System.currentTimeMillis() - startTime));
        sc.close();
    }

}
