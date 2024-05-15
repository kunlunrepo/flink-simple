package com.kl.app;

import com.kl.model.VideoOrder;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Properties;

/**
 * description : flink10--- transformation
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Flink10App {

    // 整体步骤：source -> transform -> sink
    public static void main(String[] args) throws Exception {

        // 构建执⾏任务环境以及任务的启动的⼊⼝, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.setParallelism(1);

        // source：自定义
        DataStream<VideoOrder> videoOrderDS = env.fromElements(
                new VideoOrder("21312", "java", 32, 5, new Date()),
                new VideoOrder("314", "java", 32, 5, new Date()),
                new VideoOrder("542", "springboot", 32, 5, new Date()),
                new VideoOrder("42", "redis", 32, 5, new Date()),
                new VideoOrder("52", "java", 32, 5, new Date()),
                new VideoOrder("523", "redis", 32, 5, new Date())
        );
        videoOrderDS.print();

        //处理，统计
        DataStream<Tuple2<String, Integer>> mapDS = videoOrderDS.map(new MapFunction<VideoOrder, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(VideoOrder value) throws Exception {
                return new Tuple2<>(value.getTitle(), 1);
            }
        });
        mapDS.print();

        // DataStream需要调⽤execute,可以取个名称
        env.execute("流处理 Job");

    }
}
