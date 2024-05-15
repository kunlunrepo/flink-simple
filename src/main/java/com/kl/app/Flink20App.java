package com.kl.app;

import com.kl.model.VideoOrder;
import com.kl.source.VideoOrderSource2;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * description : flink20--- window 聚合函数
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Flink20App {

    // 整体步骤：source -> transform -> sink
    public static void main(String[] args) throws Exception {

        // 构建执⾏任务环境以及任务的启动的⼊⼝, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.setParallelism(1);

        // source：自定义
        DataStreamSource<VideoOrder> videoOrderDS = env.addSource(new VideoOrderSource2());

        // transformation：keyBy
        // 分组
        KeyedStream<VideoOrder, String> keyByDS = videoOrderDS.keyBy(new KeySelector<VideoOrder, String>() {
            @Override
            public String getKey(VideoOrder value) throws Exception {
                // 以这个字段做分组
                return value.getTitle();
            }
        });
        // 5秒滚动窗口
        SingleOutputStreamOperator<VideoOrder> aggDS = keyByDS.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<VideoOrder, VideoOrder, VideoOrder>() {
                    // 初始化累加器
                    @Override
                    public VideoOrder createAccumulator() {
                        VideoOrder videoOrder = new VideoOrder();
                        return videoOrder;
                    }
                    // 加 (新，累加器)
                    @Override
                    public VideoOrder add(VideoOrder videoOrder, VideoOrder accumulator) {
                        accumulator.setMoney(accumulator.getMoney()+videoOrder.getMoney());
                        if (accumulator.getTitle() == null) {
                            accumulator.setTitle(videoOrder.getTitle());
                        }
                        if (accumulator.getCreateTime() == null) {
                            accumulator.setCreateTime(videoOrder.getCreateTime());
                        }
                        return accumulator;
                    }
                    // 获取结果 (累加器)
                    @Override
                    public VideoOrder getResult(VideoOrder o) {
                        return o;
                    }
                    // 合并 (一般不用)
                    @Override
                    public VideoOrder merge(VideoOrder o, VideoOrder acc1) {
                        VideoOrder v = new VideoOrder();
                        v.setMoney(o.getMoney()+acc1.getMoney());
                        v.setTitle(o.getTitle());
                        return v;
                    }
                });
        aggDS.print();

        // DataStream需要调⽤execute,可以取个名称
        env.execute("tumbling Job");

    }
}
