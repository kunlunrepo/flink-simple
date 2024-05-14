package com.kl.source;

import com.kl.model.VideoOrder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.*;

/**
 * description : 自定义数据源
 *
 * @author kunlunrepo
 * date :  2024-05-14 10:34
 */
public class VideoOrderSource extends RichParallelSourceFunction<VideoOrder> {

    /**
     * 是否运行
     */
    private volatile Boolean flag = true;

    /**
     * 生成编号时使用
     */
    private Random random = new Random();

    /**
     * 订单集合
     */
    private static List<String> list = new ArrayList<>();

    static {
        list.add("spring boot2.x课程");
        list.add("微服务SpringCloud课程");
        list.add("RabbitMQ消息队列");
        list.add("Kafka课程");
        list.add("⼩滴课堂⾯试专题第⼀季");
        list.add("Flink流式技术课程");
        list.add("⼯业级微服务项⽬⼤课训练营");
        list.add("Linux课程");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("--------------自定义数据源启动--------------");
    }

    @Override
    public void run(SourceContext<VideoOrder> sourceContext) throws Exception {
        while (flag) {
            Thread.sleep(1000);
            // 订单号
            String id = UUID.randomUUID().toString();
            // 用户编号
            int userId = random.nextInt(10);
            // 订单金额
            int money = random.nextInt(100);
            // 订单位置
            int videoNum = random.nextInt(list.size());
            // 订单名称
            String title = list.get(videoNum);
            // 收集订单
            sourceContext.collect(new VideoOrder(id, title, money, userId, new Date()));
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
