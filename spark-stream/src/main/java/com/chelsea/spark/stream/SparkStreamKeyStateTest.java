package com.chelsea.spark.stream;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * spark流测试类（key状态保存）
 * 
 * @author shevchenko
 *
 */
public class SparkStreamKeyStateTest {

    public static void main(String[] args) throws Exception {
        // local[2]表示有2个任务，分别执行接收数据和处理数据，如果设置为1或者不设置，将只会接收数据，处理数据代码不会执行
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamKeyStateTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 设置checkpoint保存历史数据
        //sc.setCheckpointDir("hdfs://172.18.20.237:9000/input/checkpoint/sparkStreamCheckPoint");
        sc.setCheckpointDir("C:/Users/Administrator/Desktop/SparkStreamKeyStateTest");
        // 设置日志级别
        sc.setLogLevel("error");
        // 将前5秒接收的数据批量处理
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));
        // 端口监听，linux可以安装ncat测试socket套接字输入输出，测试指令：nc -lk 9999
        // ncat安装，yum install nmap-ncat -y
        JavaReceiverInputDStream<String> socketTextStream = jsc.socketTextStream("47.107.247.223", 9999);
        JavaDStream<String> wordDS = socketTextStream.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        JavaPairDStream<String, Integer> wordPairDS = wordDS.mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                System.out.println("DS transformation算子：" + word);
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        // 将历史数据与当前批次数据做整合，并保存到checkpoint中（应用重启后历史数据丢失）
        JavaPairDStream<String, Integer> resultPairDS = wordPairDS.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> oldValue) throws Exception {
                Integer currentValue = 0;
                if (oldValue.isPresent()) {
                    currentValue = oldValue.get();
                }
                for (Integer value : values) {
                    currentValue += value;
                }
                return Optional.of(currentValue);
            }
        });
        // resultPairDS.print();
        resultPairDS.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
            
            private static final long serialVersionUID = 1L;

            @Override
            public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
                JavaPairRDD<String, Integer> mapToPair = rdd.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
                        return new Tuple2<String, Integer>(tuple._1 + "~", tuple._2);
                    }
                });
                // 必须有rdd的action算子，才能触发rdd和DS的transformation算子执行
                mapToPair.foreach(new VoidFunction<Tuple2<String,Integer>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Tuple2<String, Integer> t) throws Exception {
                        System.out.println("RDD action算子：" + t._1 + "=" + t._2);
                    }
                });
            }
        });
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

}
