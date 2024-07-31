package com.atguigu.ztest;

import com.atguigu.bean.ProvinceStats;
import com.atguigu.utils.ClickHouseUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: MyProvinceStatsSqlApp
 * Package: com.atguigu.ztest
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/26 18:34
 * @Version 1.0
 */
public class MyProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境注意跟Kafka主题分区数保持一致

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //开启CK
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //...
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));

        String orderWideTopic = "dwm_order_wide";
        String groupId = "province_stats2024";

        tableEnv.executeSql(
                "CREATE TABLE order_wide ( " +
                        "  `province_id` BIGINT, " +
                        "  `province_name` STRING, " +
                        "  `province_area_code` STRING, " +
                        "  `province_iso_code` STRING, " +
                        "  `province_3166_2_code` STRING, " +
                        "  `order_id` BIGINT, " +
                        "  `total_amount` DOUBLE, " +
                        "  `create_time` STRING, " +
                        "  rt AS TO_TIMESTAMP(create_time), " +
                        "  WATERMARK FOR rt AS rt - INTERVAL '1' SECOND " +
                        ")" + MyKafkaTool.getKafkaDDl(orderWideTopic, groupId)
        );

        Table resultTable = tableEnv.sqlQuery(
                    "SELECT " +
                        "  DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt, " +
                        "  DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt, " +
                        "  province_id, " +
                        "  province_name, " +
                        "  province_area_code, " +
                        "  province_iso_code, " +
                        "  province_3166_2_code, " +
                        "  count(order_id) order_count, " +
                        "  sum(total_amount) order_amount, " +
                        "  UNIX_TIMESTAMP() AS ts " +
                    "FROM order_wide " +
                    "GROUP BY " +
                        "  province_id, " +
                        "  province_name, " +
                        "  province_area_code, " +
                        "  province_iso_code, " +
                        "  province_3166_2_code, " +
                        "  TUMBLE(rt, INTERVAL '10' SECOND)"
        );

        DataStream<ProvinceStats> provinceStatsDS = tableEnv.toAppendStream(resultTable, ProvinceStats.class);

        provinceStatsDS.addSink(ClickHouseUtil.getSink("insert into province_stats_2024 values(?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
