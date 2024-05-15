package com.kl.app;

import com.kl.util.TimeUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * description : flink26--- state
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Flink26App {

    // 整体步骤：source -> transform -> sink
    public static void main(String[] args) throws Exception {

        // 构建执⾏任务环境以及任务的启动的⼊⼝, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.setParallelism(1);

        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir");

        // source：套接字
        DataStream<String> stringDS = env.socketTextStream("127.0.0.1", 8888);

        // transformation：state
        DataStream<Tuple3<String, String, Integer>> flatMapDS = stringDS.flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                String[] arr = value.split(",");
                // java,2024-05-15 09-10-10,15
                out.collect(Tuple3.of(arr[0], arr[1], Integer.parseInt(arr[2])));
            }
        });
        //⼀定要key by后才可以使⽤键控状态ValueState
        SingleOutputStreamOperator<Tuple2<String, Integer>> maxVideoOrder = flatMapDS.keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Integer> value) throws Exception {
                return value.f0;
            }
        }).map(new RichMapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>>() {
            // 存储状态
            private ValueState<Integer> valueState = null;
            // 初始化状态
            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("total", Integer.class));
            }
            // 计算状态
            @Override
            public Tuple2<String, Integer> map(Tuple3<String, String, Integer> tuple3) throws Exception {
                //取出State中的最⼤值
                Integer stateMaxValue = valueState.value();
                // 当前值
                Integer currentValue = tuple3.f2;
                if (stateMaxValue == null || currentValue > stateMaxValue) {
                    //更新状态,把当前的作为新的最⼤值存到状态中
                    valueState.update(currentValue);
                    return Tuple2.of(tuple3.f0, currentValue);
                } else {
                    //历史值更⼤
                    return Tuple2.of(tuple3.f0, stateMaxValue);
                }
            }
        });

        maxVideoOrder.print();

        // DataStream需要调⽤execute,可以取个名称
        env.execute("watermark Job");
    }
}
