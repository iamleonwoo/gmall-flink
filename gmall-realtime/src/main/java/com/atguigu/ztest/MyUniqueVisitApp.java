package com.atguigu.ztest;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * ClassName: MyUniqueVisitApp
 * Package: com.atguigu.ztest
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/21 2:02
 * @Version 1.0
 */
public class MyUniqueVisitApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境注意跟Kafka主题分区数保持一致

        //开启CK
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //......
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));

        String topic = "dwd_page_log";
        String groupId = "MyUniqueVisitApp2024";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaTool.getFlinkKafkaConsumer(topic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDS.map(value -> JSONObject.parseObject(value));

        KeyedStream<JSONObject, String> keyedStream = jsonObj.keyBy(value -> value.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> visitDateState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("value_state", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.hours(24L))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                visitDateState = getRuntimeContext().getState(stateDescriptor);

                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                String lastPageId = value.getJSONObject("page").getString("last_page_id");

                if (lastPageId == null) {

                    String lastVisitDate = visitDateState.value();
                    String currentDate = sdf.format(value.getLong("ts"));

                    if (lastVisitDate == null || !currentDate.equals(lastVisitDate)) {
                        visitDateState.update(currentDate);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        });

        String sinkTopic = "dwm_unique_visit";
        filterDS.map(value -> value.toJSONString())
                        .addSink(MyKafkaTool.getFlinkKafkaProducer(sinkTopic));
        env.execute();

    }
}
