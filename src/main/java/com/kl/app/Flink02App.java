package com.kl.app;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * description : flink02---批处理
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Flink02App {

    // 整体步骤：source -> transform -> sink
    public static void main(String[] args) throws Exception {

        // 构建执⾏任务环境以及任务的启动的⼊⼝, 存储全局相关的参数
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 相同类型元素的数据流
        DataSet<String> stringDS = env.fromElements("java,springboot", "java,springcloud", "kafka", "redis");
        stringDS.print("处理前");

        // FlatMapFunction<String, String>, key是输⼊类型，value是Collector响应的收集的类型，
        DataSet<String> flatMapDS = stringDS.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        // 切割
                        String[] arr = value.split(",");
                        // 收集
                        for (String word : arr) {
                            out.collect(word);
                        }
                    }
        });

        // 输出 sink
        flatMapDS.print("处理后");

        // DataStream需要调⽤execute,可以取个名称
        env.execute("批处理 Job");

    }
}
