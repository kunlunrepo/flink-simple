package com.kl.app;

import com.kl.model.VideoOrder;
import com.kl.source.VideoOrderSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * description : flink03--- source
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Flink03App {

    // 整体步骤：source -> transform -> sink
    public static void main(String[] args) throws Exception {

        // 构建执⾏任务环境以及任务的启动的⼊⼝, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // source：元素
        DataStream<String> ds1 = env.fromElements("java,springboot", "java,springcloud", "kafka", "redis");
        ds1.print("ds1");

        // source：集合
        DataStream<String> ds2 = env.fromCollection(Arrays.asList("微服务项⽬⼤课,java","alibabacloud,rabbitmq","hadoop,hbase"));
        ds2.print("ds2");

        // source：序列
        DataStream<Long> ds3 = env.fromSequence(0,10);
        ds3.print("ds3");

        // source：文件
        DataStream<String> textDS = env.readTextFile("U:info.ms-workflow.2024-05-07.101.1.2.168.0.log");
        textDS.print("textDS");

        // source：hdfs文件
//        DataStream<String> hdfsDS = env.readTextFile("hdfs://hadoop102:8020/flink/input/words.txt");
//        hdfsDS.print("hdfsDS");

        // source：socket
//        DataStream<String> socketDS = env.socketTextStream("127.0.0.1",8888);
//        socketDS.print();

        // source：自定义
        DataStream<VideoOrder> videoOrderDS = env.addSource(new VideoOrderSource());
        videoOrderDS.print();

        // DataStream需要调⽤execute,可以取个名称
        env.execute("流处理 Job");

    }
}
