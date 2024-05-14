package com.kl.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * description : 输出到redis
 *
 * @author kunlunrepo
 * date :  2024-05-14 11:24
 */
public class MyRedisSink implements RedisMapper<Tuple2<String, Integer>> {

    /**
     * 使用的命令、key
     */
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "VIDEO_ORDER_COUNTER");
    }

    /**
     * 获取key
     */
    @Override
    public String getKeyFromData(Tuple2<String, Integer> stringIntegerTuple2) {
        return stringIntegerTuple2.f0;
    }

    /**
     * 获取value
     */
    @Override
    public String getValueFromData(Tuple2<String, Integer> stringIntegerTuple2) {
        return stringIntegerTuple2.f1.toString();
    }

}
