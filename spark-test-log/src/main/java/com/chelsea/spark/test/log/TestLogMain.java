package com.chelsea.spark.test.log;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

import com.chelsea.spark.test.log.service.LogService;
import com.chelsea.spark.test.log.util.SpringUtil;

/**
 * 从kafka获取消息，统计字符串出现次数，超过5次告警
 * 
 * @author shevchenko
 *
 */
public class TestLogMain {

    private static final String brokers = "127.0.0.1:9092";
    private static final String kafkaTopics = "sparkTestLogTopic";
    private static final String groupId = "sparkTestLogGroup";
    private static final LogService logService = SpringUtil.getInstance().getBean(LogService.class);

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TestLog");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("error");
        Set<String> topicsSet = new HashSet<>(Arrays.asList(kafkaTopics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 从kafka获取指定topic消息
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
            jssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Subscribe(topicsSet, kafkaParams));
        // 将JavaInputDStream转换为JavaDStream
        JavaDStream<String> words = messages.map(new Function<ConsumerRecord<String, String>, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public String call(ConsumerRecord<String, String> v1) throws Exception {
                System.out.println("接收消息topic：" + v1.topic() + "，value：" + v1.value());
                return v1.value();
            }
            
        });
        // 将消息转换为key,value格式
        JavaPairDStream<String, Integer> wordsPair = words.mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String t) throws Exception {
                return new Tuple2<String, Integer>(t, 1);
            }
        });
        // 根据key分组计算每批次的value
        JavaPairDStream<String, Integer> reduceByKey = wordsPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        // 遍历每批次的key/value，更新入库并校验阀值
        reduceByKey.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
            
            private static final long serialVersionUID = 1L;

            @Override
            public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
                rdd.foreach(new VoidFunction<Tuple2<String,Integer>>() {
                    
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Tuple2<String, Integer> tuple) {
                        try {
                            String log = tuple._1;
                            Integer value = tuple._2;
                            logService.addOrUpdate(log, value);
                            Integer currentValue = logService.queryValueByLog(log);
                            if (currentValue > 5) {
                                System.out.println("告警！" + log + "的阀值超过5，当前值为" + currentValue);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        });
        
        jssc.start();
        jssc.awaitTermination();
    }

}
