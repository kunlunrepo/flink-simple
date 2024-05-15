package com.kl.app;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * description : flink27--- checkpoint
 *
 * @author kunlunrepo
 * date :  2024-05-13 16:24
 */
public class Flink27App {

    // 整体步骤：source -> transform -> sink
    public static void main(String[] args) throws Exception {

        // 构建执⾏任务环境以及任务的启动的⼊⼝, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.setParallelism(1);

        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir");

        //两个检查点之间间隔时间，默认是0,单位毫秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //Checkpoint过程中出现错误，是否让整体任务都失败，默认值为0，表示不容忍任何Checkpoint失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        //Checkpoint是进⾏失败恢复，当⼀个 Flink 应⽤程序失败终⽌、⼈为取消等时，它的 Checkpoint 就会被清除
        //可以配置不同策略进⾏操作
        // DELETE_ON_CANCELLATION: 当作业取消时，Checkpoint 状态信息会被删除，因此取消任务后，不能从Checkpoint 位置进⾏恢复任务
        // RETAIN_ON_CANCELLATION(多): 当作业⼿动取消时，将会保留作业的 Checkpoint 状态信息,要⼿动清除该作业的Checkpoint 状态信息
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //Flink 默认提供 Extractly-Once 保证 State 的⼀致性，还提供了 Extractly-Once，At-Least-Once 两种模式，
        //设置checkpoint的模式为EXACTLY_ONCE，也是默认的，
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint的超时时间, 如果规定时间没完成则放弃，默认是10分钟
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //设置同⼀时刻有多少个checkpoint可以同时执⾏，默认为1就⾏，以避免占⽤太多正常数据处理资源
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //设置了重启策略, 作业在失败后能⾃动恢复,失败后最多重启3次，每次重启间隔10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));

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
