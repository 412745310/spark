package com.chelsea.spark.test.log.dao;

import org.apache.ibatis.annotations.Param;

public interface LogDao {

    public void addOrUpdate(@Param("log") String log, @Param("value") Integer value);
    
    public Integer queryValueByLog(@Param("log") String log);

}
