package com.kl.app;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * description : 元组
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:10
 */
public class TupleApp {
    public static void main(String[] args) {
//        tupleTest();

        mapTest();

    }

    /**
     * map测试
     */
    private static void mapTest() {
        // 数据源
        List<String> list1 = new ArrayList<>();
        list1.add("springboot,springcloud");
        list1.add("redis6,docker");
        list1.add("kafka,rabbitmq");

        //⼀对⼀转换
        List<String> list2 = list1.stream().map(obj -> {
            obj = "⼩滴课堂" + obj;
            return obj;
        }).collect(Collectors.toList());
        System.out.println(list2);

        //⼀对多转换
        List<String> list3 = list1.stream().flatMap(
                obj -> {
                    return Arrays.stream(obj.split(","));
                }
        ).collect(Collectors.toList());
        System.out.println(list3);
    }

    /**
     * 元组测试
     */
    private static void tupleTest() {
        Tuple3<Integer, String, Double> tuple = Tuple3.of(1, "xdclass.net", 32.1);

        System.out.println(tuple.f0);
        System.out.println(tuple.f1);
        System.out.println(tuple.f2);
    }

}
