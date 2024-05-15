package com.kl.app;

import com.kl.util.TimeUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * description : flink28--- CEP 复杂事件处理
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Flink28App {

    // 整体步骤：source -> transform -> sink
    public static void main(String[] args) throws Exception {

        // 构建执⾏任务环境以及任务的启动的⼊⼝, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.setParallelism(1);

        // source：套接字
        DataStream<String> stringDS = env.socketTextStream("127.0.0.1", 8888);

        // transformation：CEP
        DataStream<Tuple3<String, String, Integer>> flatMapDS = stringDS.flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                String[] arr = value.split(",");
                // java,2024-05-15 09-10-10,15
                out.collect(Tuple3.of(arr[0], arr[1], Integer.parseInt(arr[2])));
            }
        });
        // 指定event time列
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> watermakerDS = flatMapDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        // ⽣成⼀个乱序延迟3s的固定⽔印
                        //.<Tuple3<String, String,Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        // 延迟策略去掉了延迟时间,时间是单调递增，event中的时间戳充当了⽔印
                        .<Tuple3<String, String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> {
                                    //指定POJO的事件时间列
                                    return TimeUtil.strToDate(event.f1).getTime();
                                }
                        ));
        // 分组
        KeyedStream<Tuple3<String, String, Integer>, String> keyedStream = watermakerDS.keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        // cep：定义模式
        Pattern<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>> pattern =
                Pattern
                        .<Tuple3<String, String, Integer>>begin("firstTimeLogin")
                        .where(new SimpleCondition<Tuple3<String, String, Integer>>() {
                            @Override
                            public boolean filter(Tuple3<String, String, Integer> value) throws Exception {
                                // 登录失败错误码是 -1
                                return value.f2 == -1;
                            }
                        })//.times(2).within(Time.seconds(10));//不是严格近邻
                        .next("secondTimeLogin")
                        .where(new SimpleCondition<Tuple3<String, String, Integer>>() {
                            @Override
                            public boolean filter(Tuple3<String, String, Integer> value) throws Exception {
                                return value.f2 == -1;
                            }
                        })
                        .within(Time.seconds(5));
        // cep：匹配检查
        PatternStream<Tuple3<String, String, Integer>> patternStream = CEP.pattern(keyedStream, pattern);
        SingleOutputStreamOperator<Tuple3<String, String, String>> select = patternStream.select(new PatternSelectFunction<Tuple3<String, String, Integer>, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> select(Map<String, List<Tuple3<String, String, Integer>>> map) throws Exception {
                Tuple3<String, String, Integer> firstLoginFail = map.get("firstTimeLogin").get(0);
                Tuple3<String, String, Integer> secondLoginFail = map.get("secondTimeLogin").get(0);
                return Tuple3.of(firstLoginFail.f0, firstLoginFail.f1, secondLoginFail.f1);
            }
        });
        // cep：获取结果
        select.print("匹配结果");

        // DataStream需要调⽤execute,可以取个名称
        env.execute("cep Job");
    }
}
