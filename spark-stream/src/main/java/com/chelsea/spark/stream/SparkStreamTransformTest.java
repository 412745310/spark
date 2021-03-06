package com.chelsea.spark.stream;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import scala.reflect.ClassManifestFactory;

/**
 * spark流 transform算子以及广播变量
 * 
 * @author shevchenko
 *
 */
public class SparkStreamTransformTest {
    
    // 广播变量
    private static Broadcast<List<String>> broadcast = null;
    
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamTransformTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("error");
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));
        JavaReceiverInputDStream<String> socketTextStream = jsc.socketTextStream("47.107.247.223", 9999);
        JavaPairDStream<String, String> mapToPair = socketTextStream.mapToPair(new PairFunction<String, String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                System.out.println("当前广播变量值：" + broadcast.value());
                String key = line.split(" ")[1];
                return new Tuple2<String, String>(key, line);
            }
        });
        // transform可以拿到DS中的RDD，做到RDD之间的转换
        // transform参数实现类的call方法里rdd算子之前的代码在main方法启动后会立即执行，随后会根据Durations.seconds()设置的值间隔执行
        JavaDStream<String> transform = mapToPair.transform(new Function<JavaPairRDD<String,String>, JavaRDD<String>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> rdd) throws Exception {
                System.out.println("driver端执行transform");
                // 可修改为从数据库或者配置文件动态读取
                List<String> blackList = Arrays.asList("zhangsan");
                // 广播变量赋值
                broadcast = rdd.context().broadcast(blackList, ClassManifestFactory.classType(List.class));
                JavaPairRDD<String, String> filter = rdd.filter(new Function<Tuple2<String,String>, Boolean>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        System.out.println("executor端执行filter");
                        return !broadcast.value().contains(tuple._1);
                    }
                });
                JavaRDD<String> map = filter.map(new Function<Tuple2<String,String>, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public String call(Tuple2<String, String> tuple) throws Exception {
                        return tuple._2;
                    }
                });
                return map;
            }
        });
        transform.foreachRDD(new VoidFunction<JavaRDD<String>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(JavaRDD<String> t) throws Exception {
                t.foreach(new VoidFunction<String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(String t) throws Exception {
                        System.out.println(t);
                    }
                });
            }
        });
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

}
