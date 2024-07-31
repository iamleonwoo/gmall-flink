package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.stream.Stream;

/**
 * ClassName: UniqueVisitApp
 * Package: com.atguigu.app.dwm
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/21 0:53
 * @Version 1.0
 */

//数据流：web/app -> nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> Kafka(dwm)

//程  序：mock    -> nginx -> Logger     -> Kafka(ZK)  -> BaseLogApp -> Kafka -> UniqueVisitApp -> Kafka
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境注意跟Kafka主题分区数保持一致

        //开启CK
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //......
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));

        //TODO 2.读取Kafka  dwd_page_log  主题数据
        String sourceTopic = "dwd_page_log";
        String groupId = "UniqueVisitApp2024";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId));

        //TODO 3.将数据转换为JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDS.map(value -> JSONObject.parseObject(value));

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObj.keyBy(value -> value.getJSONObject("common").getString("mid"));

        //TODO 5.使用状态编程对数据进行按天去重过滤
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> visitDateState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("visit-state", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.hours(24L))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(ttlConfig);
                visitDateState = getRuntimeContext().getState(valueStateDescriptor);
                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //取出上一跳页面信息
                String lastPageId = value.getJSONObject("page").getString("last_page_id");

                if (lastPageId == null) {
                    //取出状态数据
                    String lastVisitDate = visitDateState.value();
                    String currentDate = sdf.format(value.getLong("ts"));

                    if (lastVisitDate == null || !lastVisitDate.equals(currentDate)) {
                        //将当前日期写入状态
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

        filterDS.print();

        //TODO 6.将数据写入Kafka
        String sinkTopic = "dwm_unique_visit";
        filterDS.map(value -> value.toJSONString())
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(sinkTopic));

        //TODO 7.启动任务
        env.execute();

    }
}
