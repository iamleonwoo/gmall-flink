package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentInfo;
import com.atguigu.bean.PaymentWide;
import com.atguigu.utils.MyKafkaUtil;
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
 * ClassName: PaymentWideApp
 * Package: com.atguigu.app.dwm
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/23 22:42
 * @Version 1.0
 */

//数据流:web/app -> nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka/Phoenix(dwd/dim) -> FlinkApp -> Kafka(dwm)
//程  序:       mockDb -> Mysql -> FlinkCDCApp -> Kafka(ZK) -> BaseDbApp -> Kafka/Phoenix -> PaymentWideApp(OrderWideApp) -> Kafka
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境注意跟Kafka主题分区数保持一致

        //开启CK
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //...
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));

        //TODO 2.读取Kafka dwd_payment_info 和 dwm_order_wide两个主题的数据
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String groupId = "payment_wide_group2024";

        DataStreamSource<String> orderWideStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(paymentInfoSourceTopic, groupId));

        //TODO 3.将数据转换为JavaBean并提取时间戳生成WaterMark
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStrDS.map(value -> JSONObject.parseObject(value, OrderWide.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                                    @Override
                                    public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
                                        try {
                                            return sdf.parse(element.getCreate_time()).getTime();
                                        } catch (ParseException e) {
                                            e.printStackTrace();
                                            return recordTimestamp;
                                        }
                                    }
                                })
                );

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentStrDS.map(value -> JSONObject.parseObject(value, PaymentInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
                                        try {
                                            return sdf.parse(element.getCreate_time()).getTime();
                                        } catch (ParseException e) {
                                            e.printStackTrace();
                                            return recordTimestamp;
                                        }
                                    }
                                })
                );

        //TODO 4.双流JOIN并加工数据
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = orderWideDS.keyBy(value -> value.getOrder_id())
                .intervalJoin(paymentInfoDS.keyBy(value -> value.getOrder_id()))
                .between(Time.seconds(0), Time.minutes(15))
                .process(new ProcessJoinFunction<OrderWide, PaymentInfo, PaymentWide>() {
                    @Override
                    public void processElement(OrderWide orderWide, PaymentInfo paymentInfo, ProcessJoinFunction<OrderWide, PaymentInfo, PaymentWide>.Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        paymentWideDS.print(">>>>>>>");

        //TODO 5.将数据写入Kafka主题
        String sinkTopic = "dwm_payment_wide";
        paymentWideDS.map(value -> JSONObject.toJSONString(value))
                        .addSink(MyKafkaUtil.getFlinkKafkaProducer(sinkTopic));

        //TODO 6.启动任务
        env.execute();

    }
}
