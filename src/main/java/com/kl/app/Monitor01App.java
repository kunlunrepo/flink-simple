package com.kl.app;

import com.kl.model.AccessLogDO;
import com.kl.model.ResultCount;
import com.kl.source.AccessLogSource;
import com.kl.util.TimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * description : Monitor01App--- 监控实战案例
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Monitor01App {

    // 整体步骤：source -> transform -> sink
    public static void main(String[] args) throws Exception {

        // 构建执⾏任务环境以及任务的启动的⼊⼝, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.setParallelism(1);

        // source：自定义数据源
        DataStream<AccessLogDO> logDS = env.addSource(new AccessLogSource());

        // transformation：CEP
        // 过滤url为空的
        SingleOutputStreamOperator<AccessLogDO> filterDS = logDS.filter(new FilterFunction<AccessLogDO>() {
            @Override
            public boolean filter(AccessLogDO value) throws Exception {
                return StringUtils.isNotBlank(value.getUrl());
            }
        });

        // 指定watermark
        SingleOutputStreamOperator<AccessLogDO> watermarkDS = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        //指定允许乱序延迟的最⼤时间 3 秒
                        .<AccessLogDO>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        //指定POJO事件时间列，毫秒
                        .withTimestampAssigner((event, timestamp) -> event.getCreateTime().getTime()));

        // 最后的兜底数据
        OutputTag<AccessLogDO> lateData = new OutputTag<AccessLogDO>("lateDataLog") {
        };

        // 分组
        KeyedStream<AccessLogDO, String> keyedStream = watermarkDS.keyBy(new KeySelector<AccessLogDO, String>() {
            @Override
            public String getKey(AccessLogDO value) throws Exception {
                return value.getUrl();
            }
        });


        // 滑动窗口
        SingleOutputStreamOperator<ResultCount> tenMinPV = keyedStream
                .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(5))) //开窗
                .allowedLateness(Time.minutes(1))//允许1分钟延迟
                .sideOutputLateData(lateData)
                .aggregate(new AggregateFunction<AccessLogDO, Long, Long>() {
                    // 聚合
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }
                    // 累加
                    @Override
                    public Long add(AccessLogDO value, Long accumulator) {
                        return accumulator + 1;
                    }
                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }
                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                }, new ProcessWindowFunction<Long, ResultCount, String, TimeWindow>() {
                    // 全窗口聚合函数
                    @Override
                    public void process(String value, Context context, Iterable<Long> elements, Collector<ResultCount> out) throws Exception {
                        ResultCount resultCount = new ResultCount();
                        resultCount.setUrl(value);
                        resultCount.setType("每5秒统计近1分接⼝PV");
                        resultCount.setStartTime(TimeUtil.format(context.window().getStart()));
                        resultCount.setEndTime(TimeUtil.format(context.window().getEnd()));
                        long total = elements.iterator().next();
                        resultCount.setCount(total);
                        out.collect(resultCount);
                    }
                });
        tenMinPV.print("每5秒统计近1分接⼝PV");

        // DataStream需要调⽤execute,可以取个名称
        env.execute("Monitor Job");
    }
}
