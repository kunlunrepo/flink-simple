package com.kl.app;

import com.kl.model.VideoOrder;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * description : flink11--- transformation
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Flink11App {

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
        SingleOutputStreamOperator<String> flatMapDs = ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] arr = value.split(",");
                for (String s : arr) {
                    out.collect(s);
                }
            }
        });
        flatMapDs.print();

        // DataStream需要调⽤execute,可以取个名称
        env.execute("流处理 Job");

    }
}
