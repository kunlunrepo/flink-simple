package com.kl.app;

import com.kl.model.VideoOrder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * description : 流处理
 *
 * @author kunlunrepo
 * date :  2024-05-13 15:26
 */
public class JdkStreamApp {

    public static void main(String[] args) {
        // 订单列表1
        List<VideoOrder> orders1 = Arrays.asList(
                new VideoOrder("20190242812", "springboot教程", 3),
                new VideoOrder("20194350812", "微服务SpringCloud", 5),
                new VideoOrder("20190814232", "Redis教程", 9),
                new VideoOrder("20190523812", "⽹⻚开发教程", 9),
                new VideoOrder("201932324", "百万并发实战Netty", 9));
        // 订单列表2
        List<VideoOrder> orders2 = Arrays.asList(
                new VideoOrder("2019024285312", "springboot教程", 3),
                new VideoOrder("2019081453232", "Redis教程", 9),
                new VideoOrder("20190522338312", "⽹⻚开发教程", 9),
                new VideoOrder("2019435230812", "Jmeter压⼒测试", 5),
                new VideoOrder("2019323542411", "Git+Jenkins持续集成", 7),
                new VideoOrder("2019323542424", "Idea全套教程", 21));

        // 平均价格
        double videoOrderAvg1 =
                orders1.stream().collect(Collectors.averagingInt(VideoOrder::getMoney)).doubleValue();
        System.out.println("订单列表1平均价格="+videoOrderAvg1);

        double videoOrderAvg2 =
                orders2.stream().collect(Collectors.averagingInt(VideoOrder::getMoney)).doubleValue();
        System.out.println("订单列表2平均价格="+videoOrderAvg2);

        // 总价
        int totalMoney1 =
                orders1.stream().collect(Collectors.summingInt(VideoOrder::getMoney));
        System.out.println("订单列表1总价="+totalMoney1);

        int totalMoney2 =
                orders2.stream().collect(Collectors.summingInt(VideoOrder::getMoney));
        System.out.println("订单列表2总价="+totalMoney2);

    }
}
