package com.atguigu.ztest;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.VisitorStats;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * ClassName: MyVisitorStatsApp
 * Package: com.atguigu.ztest
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/25 2:14
 * @Version 1.0
 */
public class MyVisitorStatsApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境注意跟Kafka主题分区数保持一致

        //开启CK
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //...
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));

        //TODO 2.读取Kafka 3个主题的数据
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_app2024";

        DataStreamSource<String> pageViewDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(userJumpDetailSourceTopic, groupId));

        SingleOutputStreamOperator<VisitorStats> visitStatsWithPvDS = pageViewDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String value) throws Exception {

                        JSONObject jsonObject = JSONObject.parseObject(value);
                        JSONObject common = jsonObject.getJSONObject("common");

                        long sv = 0L;
                        if (jsonObject.getJSONObject("page").getString("last_page_id") == null) {
                            sv = 1L;
                        }

                        return new VisitorStats(
                                "",
                                "",
                                common.getString("vc"),
                                common.getString("ch"),
                                common.getString("ar"),
                                common.getString("is_new"),
                                0L,
                                1L,
                                sv,
                                0L,
                                jsonObject.getJSONObject("page").getLong("during_time"),
                                jsonObject.getLong("ts")
                        );
                    }
                }
        );

        SingleOutputStreamOperator<VisitorStats> visitStatsWithUvDS = uvDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String value) throws Exception {

                        JSONObject jsonObject = JSONObject.parseObject(value);
                        JSONObject common = jsonObject.getJSONObject("common");

                        return new VisitorStats(
                                "",
                                "",
                                common.getString("vc"),
                                common.getString("ch"),
                                common.getString("ar"),
                                common.getString("is_new"),
                                1L,
                                0L,
                                0L,
                                0L,
                                0L,
                                jsonObject.getLong("ts")
                        );
                    }
                }
        );

        SingleOutputStreamOperator<VisitorStats> visitStatsWithUjDS = ujDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String value) throws Exception {

                        JSONObject jsonObject = JSONObject.parseObject(value);
                        JSONObject common = jsonObject.getJSONObject("common");

                        return new VisitorStats(
                                "",
                                "",
                                common.getString("vc"),
                                common.getString("ch"),
                                common.getString("ar"),
                                common.getString("is_new"),
                                0L,
                                0L,
                                0L,
                                1L,
                                0L,
                                jsonObject.getLong("ts")
                        );
                    }
                }
        );

        DataStream<VisitorStats> unionDS = visitStatsWithPvDS.union(visitStatsWithUvDS, visitStatsWithUjDS);

        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWM = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<VisitorStats>() {
                                    @Override
                                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWM.keyBy(
                new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                        return new Tuple4<>(
                                value.getVc(),
                                value.getCh(),
                                value.getAr(),
                                value.getIs_new()
                        );
                    }
                }
        );

        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<VisitorStats> visitorStatsDS = windowedStream.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                        value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                        value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                        value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                        value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                        value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                        return value1;
                    }
                },
                new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {

                        VisitorStats visitorStats = input.iterator().next();

                        String stt = DateTimeUtil.toYMDhms(new Date(window.getStart()));
                        String edt = DateTimeUtil.toYMDhms(new Date(window.getEnd()));

                        visitorStats.setStt(stt);
                        visitorStats.setEdt(edt);

                        out.collect(visitorStats);
                    }
                }
        );

        visitorStatsDS.print();

        visitorStatsDS.addSink(MyClickHouseUtil.getSink("insert into visitor_stats_2024 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();

    }
}
