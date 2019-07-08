package com.chelsea.spark.stream;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * spark流获取kafka消息
 * 
 * @author shevchenko
 *
 */
public class SparkStreamKafka {

    private static final String brokers = "172.18.20.237:9092";
    private static final String kafkaTopics = "sparkTestLogTopic";
    private static final String groupId = "sparkTestLogGroup";

    public static void main(String[] args) throws Exception {
        // 构建Spark Streaming上下文
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkStreamKafka");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("error");
        jssc.checkpoint("/home/dev/tmp/checkpoint");

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("group.id", groupId);

        // 构建topic set
        String[] kafkaTopicsSplited = kafkaTopics.split(",");

        Set<String> topics = new HashSet<String>();
        for (String kafkaTopic : kafkaTopicsSplited) {
            topics.add(kafkaTopic);
        }
        JavaPairInputDStream<String, String> inputDStream =
                KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class,
                        StringDecoder.class, kafkaParams, topics);
        inputDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(JavaPairRDD<String, String> rdd) throws Exception {
                rdd.foreach(new VoidFunction<Tuple2<String, String>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Tuple2<String, String> t) throws Exception {
                        System.out.println("---------------------->" + t._1 + "=" + t._2);
                    }
                });
            }
        });
        jssc.start();
        jssc.awaitTermination();
    }

}
