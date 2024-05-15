package com.kl.model;

import com.kl.util.TimeUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * description : 订单
 *
 * @author kunlunrepo
 * date :  2024-05-13 15:21
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class VideoOrder {

    /**
     * 订单号
     */
    private String tradeNo;

    /**
     * 标题
     */
    private String title;

    /**
     * 订单金额
     */
    private int money;

    /**
     * 用户编号
     */
    private int userId;

    /**
     * 创建时间
     */
    private Date createTime;


    public VideoOrder(String tradeNo, String title, int money) {
        this.tradeNo = tradeNo;
        this.title = title;
        this.money = money;
    }

    /**
     * 打印
     */
    public String toString() {
        return "VideoOrder{" +
                "tradeNo='" + tradeNo + '\'' +
                ", title='" + title + '\'' +
                ", money=" + money +
                ", userId=" + userId +
                ", createTime=" +
                TimeUtil.format(createTime) +
                '}';
    }
}
