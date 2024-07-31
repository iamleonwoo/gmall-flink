package com.atguigu.ztest;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * ClassName: MyOrderWideApp
 * Package: com.atguigu.ztest
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/23 0:44
 * @Version 1.0
 */
public class MyOrderWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境注意跟Kafka主题分区数保持一致

        //开启CK
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //...
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));

        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String groupId = "OrderWideApp2024";

        DataStreamSource<String> orderInfoStrDS = env.addSource(MyKafkaTool.getFlinkKafkaConsumer(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailStrDS = env.addSource(MyKafkaTool.getFlinkKafkaConsumer(orderDetailSourceTopic, groupId));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoStrDS.map(value -> {

                    OrderInfo orderInfo = JSONObject.parseObject(value, OrderInfo.class);

                    String createTime = orderInfo.getCreate_time();

                    String[] dateTimeArr = createTime.split(" ");

                    orderInfo.setCreate_date(dateTimeArr[0]);
                    orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);

                    long ts = sdf.parse(createTime).getTime();
                    orderInfo.setCreate_ts(ts);

                    return orderInfo;
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                                    @Override
                                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                        return element.getCreate_ts();
                                    }
                                })
                );

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailStrDS.map(value -> {
                    OrderDetail orderDetail = JSONObject.parseObject(value, OrderDetail.class);

                    String createTime = orderDetail.getCreate_time();
                    long ts = sdf.parse(createTime).getTime();
                    orderDetail.setCreate_ts(ts);

                    return orderDetail;
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                                    @Override
                                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                        return element.getCreate_ts();
                                    }
                                })
                );

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        orderWideDS.map(new RichMapFunction<OrderWide, OrderWide>() {

            private Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                Class.forName(GmallConfig.PHOENIX_DRIVER);
                connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            }

            @Override
            public OrderWide map(OrderWide value) throws Exception {
                List<JSONObject> queryList = MyJdbcUtil.queryList(
                        connection,
                        "",
                        JSONObject.class,
                        false);

                JSONObject dimInfo = queryList.get(0);
                String userAge = dimInfo.getJSONObject("tabLeName").getString("");

//                value.setUser_age(userAge);

                return null;
            }
        });

        env.execute();

    }
}
