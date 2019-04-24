package com.chelsea.spark.stream;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;

/**
 * spark流 文件监听
 * 
 * @author shevchenko
 *
 */
public class SparkStreamFile {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkStreamFile");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));
        // 监听目录下有新增文件则触发后面的DS算子执行（原有文件内容变化，不触发DS算子执行）
        JavaDStream<String> textFileStream = jsc.textFileStream("C:/Users/Administrator/Desktop/input/file");
        JavaDStream<String> flatMap = textFileStream.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        DStream<String> dstream = flatMap.dstream();
        // 将数据保存到指定文件（file-时间戳.txt)
        dstream.saveAsTextFiles("C:/Users/Administrator/Desktop/output/file", "txt");
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

}
