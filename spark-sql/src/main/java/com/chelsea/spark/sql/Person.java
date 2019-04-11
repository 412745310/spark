package com.chelsea.spark.sql;

import java.io.Serializable;

import lombok.Data;

@Data
public class Person implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private String id;
    private String name;
    private int age;

}
