package com.kl.app;

import com.kl.model.VideoOrder;
import com.kl.source.VideoOrderSource2;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;

/**
 * description : flink22--- window 全窗口函数
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Flink22App {

    // 整体步骤：source -> transform -> sink
    public static void main(String[] args) throws Exception {

        // 构建执⾏任务环境以及任务的启动的⼊⼝, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
        SingleOutputStreamOperator<VideoOrder> processDS = keyByDS.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<VideoOrder, VideoOrder, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<VideoOrder, VideoOrder, String, TimeWindow>.Context context,
                                        Iterable<VideoOrder> in, Collector<VideoOrder> out) throws Exception {
                        List<VideoOrder> list = IteratorUtils.toList(in.iterator());
                        int total = list.stream().collect(Collectors.summingInt(VideoOrder::getMoney)).intValue();
                        VideoOrder videoOrder = new VideoOrder();
                        videoOrder.setTitle(list.get(0).getTitle());
                        videoOrder.setMoney(total);
                        videoOrder.setCreateTime(list.get(0).getCreateTime());
//                        System.out.println("开始时间：" + context.window().getStart() + " 结束时间：" + context.window().getEnd());
                        out.collect(videoOrder);
                    }
                });
        processDS.print();

        // DataStream需要调⽤execute,可以取个名称
        env.execute("tumbling Job");

    }
}
