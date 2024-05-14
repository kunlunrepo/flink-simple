package com.kl.app;

import com.kl.model.VideoOrder;
import com.kl.sink.MyRedisSink;
import com.kl.sink.MysqlSink;
import com.kl.source.VideoOrderSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * description : flink07--- sink
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Flink07App {

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

        // map转换，来⼀个记录⼀个，统计 举例：[redis, 10]
        DataStream<Tuple2<String, Integer>> mapDS = videoOrderDS.map(new MapFunction<VideoOrder, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(VideoOrder value) throws Exception {
                return new Tuple2<>(value.getTitle(), 1);
            }
        });

        // 分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mapDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        // 统计每组有多少个
        DataStream<Tuple2<String, Integer>> sumDS = keyedStream.sum(1);
        sumDS.print();

        // 输出到redis
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("192.168.10.55")
                .setPassword("base@GO5r1Ydsb6H")
                .build();
        sumDS.addSink(new RedisSink<>(conf, new MyRedisSink()));

        // DataStream需要调⽤execute,可以取个名称
        env.execute("流处理 Job");

    }
}
