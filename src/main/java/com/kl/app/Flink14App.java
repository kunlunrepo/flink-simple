package com.kl.app;

import com.kl.model.VideoOrder;
import com.kl.source.VideoOrderSource2;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description : flink14--- transformation
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Flink14App {

    // 整体步骤：source -> transform -> sink
    public static void main(String[] args) throws Exception {

        // 构建执⾏任务环境以及任务的启动的⼊⼝, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.setParallelism(1);

        // source：自定义
        DataStreamSource<VideoOrder> videoOrderDS = env.addSource(new VideoOrderSource2());

        // transformation：keyBy
        // 过滤
        DataStream<VideoOrder> filterDS = videoOrderDS.filter(new FilterFunction<VideoOrder>() {
            public boolean filter(VideoOrder value) throws Exception {
                return value.getMoney() > 20;
            }
        });
        // 分组
        KeyedStream<VideoOrder, String> videoOrderStringKeyedStream = filterDS.keyBy(new KeySelector<VideoOrder, String>() {
            @Override
            public String getKey(VideoOrder value) throws Exception {
                // 以这个字段做分组
                return value.getTitle();
            }
        });
        // 聚合
        DataStream<VideoOrder> sumDS = videoOrderStringKeyedStream.sum("money");
        sumDS.print();

        // DataStream需要调⽤execute,可以取个名称
        env.execute("流处理 Job");

    }
}
