package com.atguigu.ztest;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentInfo;
import com.atguigu.bean.PaymentWide;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * ClassName: MyPaymentWideApp
 * Package: com.atguigu.ztest
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/23 23:04
 * @Version 1.0
 */
public class MyPaymentWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境注意跟Kafka主题分区数保持一致

        //开启CK
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //...
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));

        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String groupId = "payment_wide_group2024";

        DataStreamSource<String> paymentStrDS = env.addSource(MyKafkaTool.getFlinkKafkaConsumer(paymentInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideStrDS = env.addSource(MyKafkaTool.getFlinkKafkaConsumer(orderWideSourceTopic, groupId));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentStrDS.map(value -> JSONObject.parseObject(value, PaymentInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                        try {
                                            return sdf.parse(element.getCreate_time()).getTime();
                                        } catch (ParseException e) {
                                            e.printStackTrace();
                                            return recordTimestamp;
                                        }
                                    }
                                })
                );

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStrDS.map(value -> JSONObject.parseObject(value, OrderWide.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<OrderWide>() {
                                            @Override
                                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                                try {
                                                    return sdf.parse(element.getCreate_time()).getTime();
                                                } catch (ParseException e) {
                                                    e.printStackTrace();
                                                    return recordTimestamp;
                                                }
                                            }
                                        }
                                )
                );

        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(value -> value.getOrder_id())
                .intervalJoin(orderWideDS.keyBy(value -> value.getOrder_id()))
                .between(Time.minutes(-15), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        String sinkTopic = "dwm_payment_wide";
        paymentWideDS.map(value -> JSONObject.toJSONString(value))
                .addSink(MyKafkaTool.getFlinkKafkaProducer(sinkTopic));

        env.execute();

    }
}
