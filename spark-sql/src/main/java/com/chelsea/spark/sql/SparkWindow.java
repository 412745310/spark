package com.chelsea.spark.sql;

import org.apache.spark.sql.SparkSession;

/**
 * spark开窗函数，获得一个分组内排名前几的数据
 * row_number() over (partition by xxx order by xxx desc) xxx
 * @author shevchenko
 *
 */
public class SparkWindow {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().enableHiveSupport().appName("hiveTest").getOrCreate();
        sparkSession.sql("use test1");
        sparkSession.sql("drop table if exists student_score");
        sparkSession.sql("create table if not exists student_score(id int, name string, score int) row format delimited fields terminated by ','");
        sparkSession.sql("load data local inpath '/home/dev/tmp/student_score' into table student_score");
        sparkSession.sql("select id, name, score from ("
                + "select id, name, score, row_number() over (partition by id order by score desc) rank from student_score"
                + ") t where t.rank <= 2").show();
        sparkSession.close();
    }

}
