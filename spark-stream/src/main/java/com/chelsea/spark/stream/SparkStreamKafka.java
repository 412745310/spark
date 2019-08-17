package com.chelsea.spark.stream;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

/**
 * spark流获取kafka消息
 * 
 * @author shevchenko
 *
 */
public class SparkStreamKafka {

    private static final String brokers = "127.0.0.1:9092";
    private static final String kafkaTopics = "sparkTestLogTopic";
    private static final String groupId = "sparkTestLogGroup";

    public static void main(String[] args) throws Exception {
        // 构建Spark Streaming上下文
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamKafka");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("error");
        Set<String> topicsSet = new HashSet<>(Arrays.asList(kafkaTopics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
            jssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(new Function<ConsumerRecord<String, String>, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public String call(ConsumerRecord<String, String> v1) throws Exception {
                System.out.println("接收消息topic：" + v1.topic() + "，key：" + v1.key() + "，value：" + v1.value());
                return v1.value();
            }
            
        });
        
        lines.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

}
