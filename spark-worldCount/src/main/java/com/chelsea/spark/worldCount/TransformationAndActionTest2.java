package com.chelsea.spark.worldCount;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 转换和触发类算子的分区操作
 * 返回值为RDD类型的是转换类算子，否则为触发类算子
 * 转换类算子是懒加载，必须有触发类算子才会执行
 * 
 * @author shevchenko
 *
 */
@SuppressWarnings("unused")
public class TransformationAndActionTest2 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("JavaSparkWordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 分区数为3
        JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("key1", "a"),
                new Tuple2<String, String>("key2", "b"),
                new Tuple2<String, String>("key3", "c"),
                new Tuple2<String, String>("key4", "d")
                ), 3);
        // 分区数为2
        JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("key1", "a"),
                new Tuple2<String, String>("key1", "100"),
                new Tuple2<String, String>("key2", "b"),
                new Tuple2<String, String>("key2", "100"),
                new Tuple2<String, String>("key3", "c"),
                new Tuple2<String, String>("key5", "e")
                ), 2);
        JavaRDD<String> rdd3 = sc.parallelize(Arrays.asList(
                "a", "b", "c", "d", "e", "f"
                ), 3);
        // 根据key内连接
        //join(rdd1, rdd2);
        // 根据key左连接
        //leftJoin(rdd1, rdd2);
        // 根据key右连接
        //rightJoin(rdd1, rdd2);
        // 根据key全连接
        //fullJoin(rdd1, rdd2);
        // 数据集合并
        //union(rdd1, rdd2);
        // 数据集交集
        //intersection(rdd1, rdd2);
        // 数据集差集
        //subtract(rdd1, rdd2);
        // 根据分区批量执行map
        //mapPartition(rdd3);
        // 数据集去重
        //distinct(rdd3);
        // 根据key分组
        //cogroup(rdd1, rdd2);
        // 根据分区批量执行foreach
        //foreachPartition(rdd1);
        // 根据分区索引批量执行map
        //mapPartitionsWithIndex(rdd3);
        // 重新分区，底层实现方式与coalesce(numPartitions,true)相同（宽依赖，有shuffle洗牌操作）
        //repartition(rdd3);
        // 重新分区（宽窄依赖自定义，宽依赖有shuffle洗牌操作）
        coalesce(rdd3);
        sc.close();
    }
    
    private static void coalesce(JavaRDD<String> rdd3) {
        // 如果第二个参数为true，表示需要洗牌分区，rdd之间为宽依赖
        // 如果第二个参数为false，并且重定义分区数大于原分区数，分区数以原分区数为准，不会增加
        JavaRDD<String> coalesce = rdd3.coalesce(2, true);
        int size = coalesce.partitions().size();
        System.out.println("当前分区数为:" + size);
        mapPartitionsWithIndex(coalesce);
    }

    private static void repartition(JavaRDD<String> rdd3) {
        JavaRDD<String> repartition = rdd3.repartition(2);
        mapPartitionsWithIndex(repartition);
    }

    private static void mapPartitionsWithIndex(JavaRDD<String> rdd3) {
        JavaRDD<String> mapPartitionsWithIndex = rdd3.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(Integer index, Iterator<String> iter) throws Exception {
                List<String> list = new ArrayList<String>();
                while(iter.hasNext()) {
                    String next = iter.next();
                    list.add("index:" + index + ", value:" + next);
                }
                return list.iterator();
            }
        }, true);
        mapPartitionsWithIndex.foreach(new VoidFunction<String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });
    }

    private static void foreachPartition(JavaPairRDD<String, String> rdd1) {
        int size = rdd1.partitions().size();
        System.out.println("foreachPartition分区数为：" + size);
        rdd1.foreachPartition(new VoidFunction<Iterator<Tuple2<String,String>>>() {
            
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Iterator<Tuple2<String, String>> t) throws Exception {
                int i = 0;
                while(t.hasNext()) {
                    Tuple2<String, String> next = t.next();
                    System.out.println(next);
                    i++;
                }
                System.out.println("当前分区大小为：" + i);
            }
        });
    }

    private static void cogroup(JavaPairRDD<String, String> rdd1, JavaPairRDD<String, String> rdd2) {
        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> cogroup = rdd1.cogroup(rdd2);
        int size = cogroup.partitions().size();
        System.out.println("cogroup分区数为：" + size);
        cogroup.foreach(new VoidFunction<Tuple2<String,Tuple2<Iterable<String>,Iterable<String>>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> t) throws Exception {
                System.out.println(t);
            }
        });
    }

    private static void distinct(JavaRDD<String> rdd) {
        System.out.println("执行distinct方法");
        JavaRDD<String> distinct = rdd.distinct();
        distinct.foreach(new VoidFunction<String>() {
            
            private static final long serialVersionUID = 1L;

            @Override
            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });
    }

    private static void mapPartition(JavaRDD<String> rdd) {
        JavaRDD<String> mapPartitions = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            
            private static final long serialVersionUID = 1L;
            
            @Override
            public Iterator<String> call(Iterator<String> t) throws Exception {
                List<String> list = new ArrayList<String>();
                while(t.hasNext()) {
                    String s = t.next() + "~";
                    list.add(s);
                }
                System.out.println("当前分区list大小：" + list.size());
                return list.iterator();
            }
        });
        mapPartitions.foreach(new VoidFunction<String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });
    }

    private static void subtract(JavaPairRDD<String, String> rdd1, JavaPairRDD<String, String> rdd2) {
        JavaPairRDD<String, String> subtract = rdd1.subtract(rdd2);
        int size = subtract.partitions().size();
        System.out.println("subtract分区数为：" + size);
        subtract.foreach(new VoidFunction<Tuple2<String,String>>() {
            
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, String> t) throws Exception {
                System.out.println(t);
            }
        });
    }

    private static void intersection(JavaPairRDD<String, String> rdd1, JavaPairRDD<String, String> rdd2) {
        JavaPairRDD<String, String> intersection = rdd1.intersection(rdd2);
        int size = intersection.partitions().size();
        System.out.println("intersection分区数为：" + size);
        intersection.foreach(new VoidFunction<Tuple2<String,String>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, String> t) throws Exception {
                System.out.println(t);
            }
        });
    }

    private static void union(JavaPairRDD<String, String> rdd1, JavaPairRDD<String, String> rdd2) {
        JavaPairRDD<String, String> union = rdd1.union(rdd2);
        int size = union.partitions().size();
        System.out.println("union分区数为：" + size);
        union.foreach(new VoidFunction<Tuple2<String,String>>() {
            
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, String> t) throws Exception {
                System.out.println(t);
            }
        });
    }

    private static void fullJoin(JavaPairRDD<String, String> rdd1, JavaPairRDD<String, String> rdd2) {
        JavaPairRDD<String, Tuple2<Optional<String>, Optional<String>>> fullOuterJoin = rdd1.fullOuterJoin(rdd2);
        int size = fullOuterJoin.partitions().size();
        System.out.println("fullJoin分区数为：" + size);
        fullOuterJoin.foreach(new VoidFunction<Tuple2<String,Tuple2<Optional<String>,Optional<String>>>>() {
            
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Tuple2<Optional<String>, Optional<String>>> t) throws Exception {
                System.out.println(t);
            }
        });
    }

    private static void rightJoin(JavaPairRDD<String, String> rdd1, JavaPairRDD<String, String> rdd2) {
        JavaPairRDD<String, Tuple2<Optional<String>, String>> join = rdd1.rightOuterJoin(rdd2);
        int size = join.partitions().size();
        System.out.println("rightJoin分区数为：" + size);
        join.foreach(new VoidFunction<Tuple2<String,Tuple2<Optional<String>,String>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Tuple2<Optional<String>, String>> t) throws Exception {
                String key = t._1;
                Optional<String> value1 = t._2._1;
                String value2 = t._2._2;
                String value = null;
                if(value1.isPresent()) {
                    value = value1.get();
                }
                System.out.println("("+ key +",("+ value +","+ value2 +"))");
            }
        });
    }

    private static void leftJoin(JavaPairRDD<String, String> rdd1, JavaPairRDD<String, String> rdd2) {
        JavaPairRDD<String, Tuple2<String, Optional<String>>> join = rdd1.leftOuterJoin(rdd2);
        int size = join.partitions().size();
        System.out.println("leftJoin分区数为：" + size);
        join.foreach(new VoidFunction<Tuple2<String,Tuple2<String, Optional<String>>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Tuple2<String, Optional<String>>> t) throws Exception {
                String key = t._1;
                String value1 = t._2._1;
                Optional<String> value2 = t._2._2;
                String value = null;
                if(value2.isPresent()) {
                    value = value2.get();
                }
                System.out.println("("+ key +",("+ value1 +","+ value +"))");
            }
            
        });
    }

    private static void join(JavaPairRDD<String, String> rdd1, JavaPairRDD<String, String> rdd2) {
        JavaPairRDD<String, Tuple2<String, String>> join = rdd1.join(rdd2);
        int size = join.partitions().size();
        System.out.println("join分区数为：" + size);
        join.foreach(new VoidFunction<Tuple2<String,Tuple2<String,String>>>() {

            private static final long serialVersionUID = 1L;
            
            @Override
            public void call(Tuple2<String, Tuple2<String, String>> t) throws Exception {
                System.out.println(t);
            }
        });
    }
    
}
