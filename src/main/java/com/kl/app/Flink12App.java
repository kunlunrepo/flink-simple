package com.kl.app;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * description : flink12--- transformation
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Flink12App {

    // 整体步骤：source -> transform -> sink
    public static void main(String[] args) throws Exception {

        // 构建执⾏任务环境以及任务的启动的⼊⼝, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.setParallelism(1);

        // source：自定义
        DataStream<String> ds = env.fromElements(
                "spring,java", "springcloud,flink", "java,kafka"
        );
//        ds.print();

        //处理，统计 flatmap会展开
        SingleOutputStreamOperator<String> richMap = ds.map(new RichMapFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("******richMap-open******");
            }

            @Override
            public void close() throws Exception {
                System.out.println("******richMap-close******");
            }

            @Override
            public String map(String s) throws Exception {
                return "richmap:" + s;
            }
        });
//        richMap.print();

        SingleOutputStreamOperator<String> flatMap = ds.flatMap(new RichFlatMapFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("******richFlatMap-open******");
            }

            @Override
            public void close() throws Exception {
                System.out.println("******richFlatMap-close******");
            }

            @Override
            public void flatMap(String s, Collector<String> out) throws Exception {
                String[] split = s.split(",");
                for (String s1 : split) {
                    out.collect("flatmap:" + s1);
                }
            }
        });
        flatMap.print();

        // DataStream需要调⽤execute,可以取个名称
        env.execute("流处理 Job");

    }
}
