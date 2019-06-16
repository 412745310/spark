package com.chelsea.spark.core.Accumulator;

import org.apache.spark.util.AccumulatorV2;

import com.chelsea.spark.core.utils.StringUtils;

/**
 * 自动以累加器
 * isZero: 当AccumulatorV2中存在类似数据不存在这种问题时，是否结束程序。 
 * copy: 拷贝一个新的AccumulatorV2 
 * reset: 重置AccumulatorV2中的数据 
 * add: 操作数据累加方法实现 
 * merge: 合并分区数据（有几个分区就执行几次）
 * value: AccumulatorV2对外访问的数据结果
 * @author shevchenko
 *
 */
public class MyAccumulatorV2 extends AccumulatorV2<String, String> {

    private static final long serialVersionUID = 1L;

    // 定义要拼接成的字符串的格式
    String str = "A=0|B=0|C=0|D=0|E=0|F=0|G=0|H=0|I=0";

    @Override
    public void add(String v) {
        String oldValues = StringUtils.getFieldFromConcatString(str, "\\|", v);
        int newValues = Integer.valueOf(oldValues) + 1;
        String newString = StringUtils.setFieldInConcatString(str, "\\|", v, String.valueOf(newValues));
        str = newString;
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        MyAccumulatorV2 newAccumulator = new MyAccumulatorV2();
        newAccumulator.str = this.str;
        return newAccumulator;
    }

    @Override
    public boolean isZero() {
        return str == "A=0|B=0|C=0|D=0|E=0|F=0|G=0|H=0|I=0";
    }

    @Override
    public void merge(AccumulatorV2<String, String> other) {
        MyAccumulatorV2 o = (MyAccumulatorV2) other;
        String[] words = str.split("\\|");
        String[] owords = o.str.split("\\|");
        for (int i = 0; i < words.length; i++) {
            for (int j = 0; j < owords.length; j++) {
                if (words[i].split("=")[0].equals(owords[j].split("=")[0])) {
                    int value = Integer.valueOf(words[i].split("=")[1]) + Integer.valueOf(owords[j].split("=")[1]);
                    String ns =
                            StringUtils.setFieldInConcatString(str, "\\|", owords[j].split("=")[0],
                                    String.valueOf(value));
                    // 每次合并完，更新str
                    str = ns;
                }
            }
        }
    }

    @Override
    public void reset() {
        str = "A=0|B=0|C=0|D=0|E=0|F=0|G=0|H=0|I=0";
    }

    @Override
    public String value() {
        return str;
    }

}
