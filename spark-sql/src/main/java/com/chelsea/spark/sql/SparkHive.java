package com.chelsea.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * hive读取与保存
 * 
 * @author shevchenko
 *
 */
public class SparkHive {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().enableHiveSupport().appName("hiveTest").getOrCreate();
        sparkSession.sql("use test1");
        sparkSession.sql("drop table if exists student_infos");
        sparkSession.sql("create table if not exists student_infos(name string, score int) row format delimited fields terminated by ','");
        sparkSession.sql("load data local inpath '/home/dev/tmp/student_infos' into table student_infos");
        // spark加载hive正式表数据
        Dataset<Row> sql = sparkSession.sql("select si.name, si.score from student_infos si where si.score >= 80");
        SQLContext sqlContext = sparkSession.sqlContext();
        sqlContext.registerDataFrameAsTable(sql, "goodstudent");
        // spark加载临时表数据
        Dataset<Row> sql2 = sparkSession.sql("select * from goodstudent");
        sql2.show();
        
        sparkSession.sql("drop table if exists good_student_infos");
        // spark保存数据到hive正式表
        sql.write().mode(SaveMode.Overwrite).saveAsTable("good_student_infos");
        // spark加载hive正式表数据
        Dataset<Row> table = sparkSession.table("good_student_infos");
        table.show();
        
        sparkSession.close();
    }

}
