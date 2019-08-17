package com.chelsea.spark.test.log.util;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * spring工具类
 * 
 * @author shevchenko
 *
 */
public class SpringUtil {
    
    private SpringUtil () {}

    private static class SpringUtilInner {

        private static ApplicationContext context = null;

        static {
            context = new ClassPathXmlApplicationContext("classpath*:spring.xml");
        }

    }

    public static ApplicationContext getInstance() {
        return SpringUtilInner.context;
    }

}
