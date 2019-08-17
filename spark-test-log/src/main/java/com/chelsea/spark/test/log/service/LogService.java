package com.chelsea.spark.test.log.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.chelsea.spark.test.log.dao.LogDao;

@Service
public class LogService {
    
    @Autowired
    private LogDao logDao;
    
    public void addOrUpdate(String log, Integer value) {
        try {
            logDao.addOrUpdate(log, value);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }
    
    public Integer queryValueByLog(String log) {
        return logDao.queryValueByLog(log);
    }

}
