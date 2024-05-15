package com.kl.app;

import com.kl.model.VideoOrder;
import com.kl.sink.MyRedisSink;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.util.Date;
import java.util.Properties;

/**
 * description : flink08--- source
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Flink08App {

    // 整体步骤：source -> transform -> sink
    public static void main(String[] args) throws Exception {

        // 构建执⾏任务环境以及任务的启动的⼊⼝, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.setParallelism(1);

        // kafka属性
        Properties props = new Properties();
        // kafka集群地址
        props.setProperty("bootstrap.servers", "192.168.10.55:9092");
        // 消费组
        props.setProperty("group.id", "video-order-group");
        // 字符串序列化和反序列化
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // offset重置规则
        props.setProperty("auto.offset.reset", "latest");
        // 自动提交
        props.setProperty("enable.auto.commit", "true");
        // 提交间隔 每2秒提交一次，减少资源消耗
        props.setProperty("auto.commit.interval.ms", "2000");
        // 每个10检测一下kafka的分区变化情况
        props.setProperty("flink.partition-discovery.interval-millis", "10000");

        // kafka消费者
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "xdclass-topic",
                new SimpleStringSchema(),
                props
        );
        // 设置消费者启动位置
        kafkaConsumer.setStartFromGroupOffsets();

        // source：kafka
        DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);
        kafkaDS.print();

        // DataStream需要调⽤execute,可以取个名称
        env.execute("流处理 Job");

    }
}
