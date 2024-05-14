package com.kl.app;

import com.kl.model.VideoOrder;
import com.kl.sink.MysqlSink;
import com.kl.source.VideoOrderSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description : flink06--- sink
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Flink06App {

    // 整体步骤：source -> transform -> sink
    public static void main(String[] args) throws Exception {

        // 构建执⾏任务环境以及任务的启动的⼊⼝, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.setParallelism(2);

        // source：自定义
        DataStream<VideoOrder> videoOrderDS = env.addSource(new VideoOrderSource());
        videoOrderDS.print();

        DataStream<VideoOrder> filterDS = videoOrderDS.filter(new FilterFunction<VideoOrder>()
                {
                    @Override
                    public boolean filter(VideoOrder videoOrder) throws Exception {
                        return videoOrder.getMoney()>5;
                    }
                });
        filterDS.print();

        // sink：自定义
        filterDS.addSink(new MysqlSink());

        // DataStream需要调⽤execute,可以取个名称
        env.execute("流处理 Job");

    }
}
