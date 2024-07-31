package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentWide;
import com.atguigu.bean.ProductStats;
import com.atguigu.common.GmallConstant;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: ProductStatsApp
 * Package: com.atguigu.app.dws
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/25 23:27
 * @Version 1.0
 */
public class ProductStatsApp {
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

        //TODO 2.读取Kafka数据
        String pageViewSourceTopic = "dwd_page_log";
        String favorInfoSourceTopic = "dwd_favor_info";
        String cartInfoSourceTopic = "dwd_cart_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        String groupId = "product_stats_app";

        DataStreamSource<String> pageViewDStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> favorInfoDStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(favorInfoSourceTopic, groupId));
        DataStreamSource<String> cartInfoDStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(cartInfoSourceTopic, groupId));
        DataStreamSource<String> oderrWideDStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentWideDStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(paymentWideSourceTopic, groupId));
        DataStreamSource<String> refundInfoDStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commentInfoDStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(commentInfoSourceTopic, groupId));

        //TODO 3.统一数据格式
        //3.1 整理pageViewDStream
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pageViewDStream.flatMap(
                new FlatMapFunction<String, ProductStats>() {
                    @Override
                    public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                        //将数据转换为JSONObject
                        JSONObject jsonObject = JSONObject.parseObject(value);

                        //取出数据中的ts
                        Long ts = jsonObject.getLong("ts");

                        //取出点击数据
                        JSONObject page = jsonObject.getJSONObject("page");
                        String pageId = page.getString("page_id");
                        String itemType = page.getString("item_type");
                        if ("good_detail".equals(pageId) && "sku_id".equals(itemType)) {
                            //写出一条点击数据
                            out.collect(
                                    ProductStats.builder()
                                            .sku_id(page.getLong("item"))
                                            .click_ct(1L)
                                            .ts(ts)
                                            .build()
                            );
                        }

                        //取出曝光数据
                        JSONArray displays = jsonObject.getJSONArray("displays");
                        if (displays != null && displays.size() > 0) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject display = displays.getJSONObject(i);
                                if ("sku_id".equals(display.getString("item_type"))) {
                                    //写出一条曝光数据
                                    out.collect(
                                            ProductStats.builder()
                                                    .sku_id(display.getLong("item"))
                                                    .display_ct(1L)
                                                    .ts(ts)
                                                    .build()
                                    );
                                }
                            }
                        }
                    }
                }
        );

        //3.2 收藏数据
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDs = favorInfoDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String value) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));

                        return ProductStats.builder()
                                .sku_id(jsonObject.getLong("sku_id"))
                                .favor_ct(1L)
                                .ts(ts)
                                .build();
                    }
                }
        );

        //3.3 加购数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartInfoDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String value) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));

                        return ProductStats.builder()
                                .sku_id(jsonObject.getLong("sku_id"))
                                .cart_ct(1L)
                                .ts(ts)
                                .build();
                    }
                }
        );

        //3.4 下单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = oderrWideDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String value) throws Exception {

                        OrderWide orderWide = JSONObject.parseObject(value, OrderWide.class);

                        HashSet<Long> orderIds = new HashSet<>();
                        orderIds.add(orderWide.getOrder_id());

                        Long ts = DateTimeUtil.toTs(orderWide.getCreate_time());

                        return ProductStats.builder()
                                .sku_id(orderWide.getSku_id())
                                .order_sku_num(orderWide.getSku_num())
                                .order_amount(orderWide.getTotal_amount())
                                .orderIdSet(orderIds)
                                .ts(ts)
                                .build();
                    }
                }
        );

        //3.5 支付数据
        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = paymentWideDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String value) throws Exception {

                        PaymentWide paymentWide = JSONObject.parseObject(value, PaymentWide.class);

                        HashSet<Long> paidOrderIds = new HashSet<>();
                        paidOrderIds.add(paymentWide.getOrder_id());

                        Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());

                        return ProductStats.builder()
                                .sku_id(paymentWide.getSku_id())
                                .payment_amount(paymentWide.getTotal_amount())
                                .paidOrderIdSet(paidOrderIds)
                                .ts(ts)
                                .build();
                    }
                }
        );

        //3.6 退款数据
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundInfoDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String value) throws Exception {

                        JSONObject jsonObject = JSONObject.parseObject(value);

                        HashSet<Long> refundOrderIds = new HashSet<>();
                        refundOrderIds.add(jsonObject.getLong("order_id"));

                        Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));

                        return ProductStats.builder()
                                .sku_id(jsonObject.getLong("sku_id"))
                                .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                                .refundOrderIdSet(refundOrderIds)
                                .ts(ts)
                                .build();
                    }
                }
        );

        //3.7 评价数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentInfoDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String value) throws Exception {

                        JSONObject jsonObject = JSONObject.parseObject(value);

                        //取出评价类型 1201 1202 1203 1204...
                        String appraise = jsonObject.getString("appraise");

                        long ct = 0L;
                        if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                            ct = 1L;
                        }

                        Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));

                        return ProductStats.builder()
                                .sku_id(jsonObject.getLong("sku_id"))
                                .comment_ct(1L)
                                .good_comment_ct(ct)
                                .ts(ts)
                                .build();
                    }
                }
        );

        //TODO 4.Union
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(
                productStatsWithFavorDs,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPaymentDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS);

        //TODO 5.提取时间戳生成WaterMark
        SingleOutputStreamOperator<ProductStats> productStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<ProductStats>() {
                                    @Override
                                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        //TODO 6.分组、开窗&聚合
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuKeyDS = productStatsWithWMDS.keyBy(value -> value.getSku_id())
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(
                        new ReduceFunction<ProductStats>() {
                            @Override
                            public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {

                                stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                                stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                                stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                                stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());

                                stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                                stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                                stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
//                                                stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);

                                stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));
                                stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
//                                                stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                                stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));
                                stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
//                                                stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);

                                stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                                stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                                return stats1;
                            }
                        },
                        new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                            @Override
                            public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {

                                //取出窗口时间
                                String stt = DateTimeUtil.toYMDhms(new Date(window.getStart()));
                                String edt = DateTimeUtil.toYMDhms(new Date(window.getEnd()));

                                //赋值窗口信息
                                ProductStats productStats = input.iterator().next();
                                productStats.setStt(stt);
                                productStats.setEdt(edt);

                                //赋值订单个数、支付订单个数、退款订单个数
                                productStats.setOrder_ct(productStats.getOrderIdSet().size() + 0L);
                                productStats.setPaid_order_ct(productStats.getPaidOrderIdSet().size() + 0L);
                                productStats.setRefund_order_ct(productStats.getRefundOrderIdSet().size() + 0L);

                                //返回数据
                                out.collect(productStats);
                            }
                        }
                );

        //TODO 7.关联维度信息
        //7.1 关联SKU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuInfoDS = AsyncDataStream.unorderedWait(
                productStatsWithSkuKeyDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getId(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {

                        productStats.setSku_name(dimInfo.getString("SKU_NAME"));
                        productStats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                        productStats.setSpu_id(dimInfo.getLong("SPU_ID"));
                        productStats.setTm_id(dimInfo.getLong("TM_ID"));
                        productStats.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                    }
                },
                60L,
                TimeUnit.SECONDS
        );

        //7.2 补充SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream = AsyncDataStream.unorderedWait(
                productStatsWithSkuInfoDS,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getId(ProductStats productStats) {
                        return productStats.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                        productStats.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }
                },
                60L,
                TimeUnit.SECONDS
        );

        //7.3 补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream = AsyncDataStream.unorderedWait(
                productStatsWithSpuDstream,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getId(ProductStats productStats) {
                        return productStats.getCategory3_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                        productStats.setCategory3_name(dimInfo.getString("NAME"));
                    }
                },
                60L,
                TimeUnit.SECONDS
        );

        //7.4 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream = AsyncDataStream.unorderedWait(
                productStatsWithCategory3Dstream,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getId(ProductStats productStats) {
                        return productStats.getTm_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                        productStats.setTm_name(dimInfo.getString("TM_NAME"));
                    }
                },
                60L,
                TimeUnit.SECONDS
        );

        productStatsWithTmDstream.print();

        //TODO 8.写入ClickHouse
        productStatsWithTmDstream.addSink(ClickHouseUtil.getSink("insert into product_stats_2024 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute();
    }
}
