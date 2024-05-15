package com.kl.app;

import com.kl.model.VideoOrder;
import com.kl.source.VideoOrderSource2;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;

/**
 * description : flink16--- transformation
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Flink16App {

    // 整体步骤：source -> transform -> sink
    public static void main(String[] args) throws Exception {

        // 构建执⾏任务环境以及任务的启动的⼊⼝, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);


        // source：自定义
        DataStream<VideoOrder> videoOrderDS = env.fromElements(
                new VideoOrder("1", "java", 31, 15, new Date()),
                new VideoOrder("2", "java", 32, 45, new Date()),
                new VideoOrder("3", "java", 33, 52, new Date()),
                new VideoOrder("4", "springboot", 21, 5, new Date()),
                new VideoOrder("5", "redis", 41, 52, new Date()),
                new VideoOrder("6", "redis", 40, 15, new Date()),
                new VideoOrder("7", "kafka", 1, 55, new Date())
        );


        // transformation
        // maxBy 整个对象的属性都是最新的
        SingleOutputStreamOperator<VideoOrder> maxByDS = videoOrderDS.keyBy(new KeySelector<VideoOrder, String>() {
            @Override
            public String getKey(VideoOrder value) throws Exception {
                // 以这个字段做分组
                return value.getTitle();
            }
        }).maxBy("money");
        maxByDS.print("maxBy:");

        // max 对象的其他属性是错误的
        SingleOutputStreamOperator<VideoOrder> maxDS = videoOrderDS.keyBy(new KeySelector<VideoOrder, String>() {
            @Override
            public String getKey(VideoOrder value) throws Exception {
                // 以这个字段做分组
                return value.getTitle();
            }
        }).max("money");
//        maxDS.print("max:");

        // DataStream需要调⽤execute,可以取个名称
        env.execute("流处理 Job");
    }
}
