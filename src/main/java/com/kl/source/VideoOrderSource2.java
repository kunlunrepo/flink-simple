package com.kl.source;

import com.kl.model.VideoOrder;
import com.kl.util.TimeUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.*;

/**
 * description : 自定义数据源
 *
 * @author kunlunrepo
 * date :  2024-05-14 10:34
 */
public class VideoOrderSource2 extends RichParallelSourceFunction<VideoOrder> {

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
    private static List<VideoOrder> list = new ArrayList<>();

    static {
        list.add(new VideoOrder("", "java", 10, 5, new Date()));
        list.add(new VideoOrder("", "spring boot", 30, 5, new Date()));
//        list.add(new VideoOrder("", "spring cloud", 20, 5, new Date()));
//        list.add(new VideoOrder("", "flink", 30, 5, new Date()));
//        list.add(new VideoOrder("", "面试专题第一季", 30, 5, new Date()));
//        list.add(new VideoOrder("", "项目大课", 1, 5, new Date()));
//        list.add(new VideoOrder("", "kafka", 30, 5, new Date()));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("--------------open--------------");
    }

    @Override
    public void run(SourceContext<VideoOrder> sourceContext) throws Exception {
        while (flag) {
            Thread.sleep(1000);
            // 订单号
            String id = UUID.randomUUID().toString().substring(30);
            // 用户编号
            int userId = random.nextInt(10);
            // 订单位置
            int videoNum = random.nextInt(list.size());
            // 订单名称
            VideoOrder videoOrder = list.get(videoNum);
            videoOrder.setTradeNo(id);
            videoOrder.setUserId(userId);
            videoOrder.setCreateTime(new Date());
            System.out.println("产⽣:"+videoOrder.getTitle()+"，价格:"+videoOrder.getMoney()+", 时间:"+ TimeUtil.format(videoOrder.getCreateTime()));
            // 收集订单
            sourceContext.collect(videoOrder);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
