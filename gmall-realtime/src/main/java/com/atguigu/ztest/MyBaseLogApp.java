package com.atguigu.ztest;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * ClassName: MyBaseLogApp
 * Package: com.atguigu.ztest
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/18 21:42
 * @Version 1.0
 */
public class MyBaseLogApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //......
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));

        String topic = "ods_base_log";
        String groupId = "MyBaseLogApp2024";
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaTool.getFlinkKafkaConsumer(topic, groupId));

        OutputTag<String> dirtyDateTag = new OutputTag<String>("DirtyData") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStreamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyDateTag, value);
                }
            }
        });

        jsonObjDS.getSideOutput(dirtyDateTag).print("Dirty>>>>>>>");

        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonobj -> jsonobj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> firstVisitDateState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDateState", String.class));
                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String is_new = value.getJSONObject("common").getString("is_new");

                if ("1".equals(is_new)) {
                    String firstVisitDate = firstVisitDateState.value();
                    if (firstVisitDate != null) {
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        Long ts = value.getLong("ts");
                        firstVisitDateState.update(sdf.format(ts));
                    }
                }
                return value;
            }
        });

        OutputTag<JSONObject> startTag = new OutputTag<JSONObject>("start") {
        };
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {
        };

        SingleOutputStreamOperator<JSONObject> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                String start = value.getString("start");
                if (start != null && start.length() > 0) {
                    ctx.output(startTag, value);
                } else {
                    out.collect(value);

                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        String pageId = value.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);
                            ctx.output(displayTag, display);
                        }
                    }
                }
            }
        });

        DataStream<JSONObject> startDS = pageDS.getSideOutput(startTag);
        DataStream<JSONObject> displayDS = pageDS.getSideOutput(displayTag);

        pageDS.print("page>>>>>");
        startDS.print("start>>>>>");
        displayDS.print("display>>>>>");

        String pageTopic = "dwd_page_log";
        String startTopic = "dwd_start_log";
        String displayTopic = "dwd_display_log";

        pageDS.map(value -> value.toJSONString()).addSink(MyKafkaTool.getFlinkKafkaProducer(pageTopic));
        startDS.map(value -> value.toJSONString()).addSink(MyKafkaTool.getFlinkKafkaProducer(startTopic));
        displayDS.map(value -> value.toJSONString()).addSink(MyKafkaTool.getFlinkKafkaProducer(displayTopic));

        env.execute();
    }
}
