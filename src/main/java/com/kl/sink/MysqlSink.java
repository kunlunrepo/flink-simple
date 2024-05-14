package com.kl.sink;

import com.kl.model.VideoOrder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * description : 输出到mysql
 *
 * @author kunlunrepo
 * date :  2024-05-14 11:05
 */
public class MysqlSink extends RichSinkFunction<VideoOrder> {

    // 连接
    private Connection conn = null;

    // 预编译sql
    private PreparedStatement ps = null;

    /**
     * 打开 加载驱动，创建连接
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://192.168.10.55:3306/xdclass_order?useUnicode=true&characterEncoding=utf8&allowMultiQueries=true&serverTimezone=Asia/Shanghai",
                "root",
        "base@GO5r1Ydsb6H");
        String sql = "INSERT INTO `video_order` (`user_id`, `money`, `title`, `trade_no`, `create_time`) VALUES(?,?,?,?,?);";
        ps = conn.prepareStatement(sql);
    }


    /**
     * 执行
     */
    @Override
    public void invoke(VideoOrder videoOrder, Context context) throws Exception {
        //给ps中的?设置具体值
        ps.setInt(1,videoOrder.getUserId());
        ps.setInt(2,videoOrder.getMoney());
        ps.setString(3,videoOrder.getTitle());
        ps.setString(4,videoOrder.getTradeNo());
        ps.setDate(5,new Date(videoOrder.getCreateTime().getTime()));
        // 执行sql
        ps.executeUpdate();
    }


    /**
     * 关闭
     */
    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
        if (ps != null) {
            ps.close();
        }
    }
}
