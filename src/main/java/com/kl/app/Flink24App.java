package com.kl.app;

import com.kl.util.TimeUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * description : flink24--- watermark 水印 二次延迟
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Flink24App {

    // 整体步骤：source -> transform -> sink
    public static void main(String[] args) throws Exception {

        // 构建执⾏任务环境以及任务的启动的⼊⼝, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.setParallelism(1);

        // source：套接字
        DataStream<String> stringDS = env.socketTextStream("127.0.0.1", 8888);

        // transformation：watermark
        DataStream<Tuple3<String, String, Integer>> flatMapDS = stringDS.flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                String[] arr = value.split(",");
                // java,2024-05-15 09-10-10,15
                out.collect(Tuple3.of(arr[0], arr[1], Integer.parseInt(arr[2])));
            }
        });
        // 添加watermark
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> watermakerDS = flatMapDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple3<String, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3)) //指定最⼤允许的延迟/乱序 时间
                        .withTimestampAssigner((event, timestamp) -> {
                            //指定POJO的事件时间列
                            return TimeUtil.strToDate(event.f1).getTime();
                        }));
        // 滚动窗口
        SingleOutputStreamOperator<String> sumDS = watermakerDS.keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                // 开窗 (对于事件时间，窗口的开始和结束时间是由数据中的时间戳决定的)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 允许1分钟延迟
                .allowedLateness(Time.minutes(1))
                // 聚合
                .apply(new WindowFunction<Tuple3<String, String, Integer>, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple3<String, String, Integer>> input, Collector<String> out) throws Exception {
                        //存放窗⼝的数据的事件时间
                        List<String> eventTimeList = new ArrayList<>();
                        int total = 0;
                        for (Tuple3<String, String, Integer> order : input) {
                            eventTimeList.add(order.f1);
                            total = total + order.f2; // 聚合金额
                        }
                        String outStr = String.format("分组key:%s,聚合值:%s,窗⼝开始结束:[%s~%s),窗⼝所有事件时间:%s", key, total, TimeUtil.format(window.getStart()), TimeUtil.format(window.getEnd()), eventTimeList);
                        out.collect(outStr);
                    }
                });
        sumDS.print();

        // DataStream需要调⽤execute,可以取个名称
        env.execute("watermark Job");
    }
}
