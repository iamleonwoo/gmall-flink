package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.MyStringDeserializationSchema;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * ClassName: BaseDbApp
 * Package: com.atguigu.app.dwd
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/19 1:10
 * @Version 1.0
 */

//数据流:web/app -> Nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka(dwd)/Phoenix(dim)

//程  序:          mock                 -> Mysql -> FlinkCDCApp -> Kafka(ZK) -> BaseDbApp -> Kafka/Phoenix(ZK,HDFS,HBase)

public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境注意跟Kafka主题分区数保持一致

        //开启CK
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //......
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck/"));

        //TODO 2.读取Kafka ods_base_db 主题数据
        String topic = "ods_base_db";
        String groupId = "BaseDbApp2024";
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.将每行数据转换为JSONObject           主流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStreamSource.map(value -> JSONObject.parseObject(value))
                .filter(value -> {
                    return !"delete".equals(value.getString("type"));
                });

        //TODO 4.通过FlinkCDC读取配置信息表,并封装为  广播流
        DebeziumSourceFunction<String> tableProcessStrSourceFunc = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-210225-realtime")
                .tableList("gmall-210225-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyStringDeserializationSchema())
                .build();

        DataStreamSource<String> tableProcessStrDS = env.addSource(tableProcessStrSourceFunc);

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);

        //TODO 5.连接主流和广播流
        //主流: {"database":"","tableName":"base_trademark","data":{"id":"","tm_name":"","logo_url":""},"before":{},"type":"insert"}
        //广播流: {"database":"","tableName":"table_process","data":{"sourceTable":"","operateType":"",...,},"before":{},"type":"insert"}
        BroadcastConnectedStream<JSONObject, String> broadcastConnectedStream = jsonObjDS.connect(broadcastStream);

        //TODO 6.处理连接流数据
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase") {
        };
        SingleOutputStreamOperator<JSONObject> kafkaDS = broadcastConnectedStream.process(new TableProcessFunction(mapStateDescriptor, hbaseTag));

        //TODO 7.获取Kafka数据流以及HBASE数据流写入对应的存储框架中
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);

        kafkaDS.print("Kafka>>>>>>>");
        hbaseDS.print("HBase>>>>>>>");

        //element:{"database":"","tableName":"base_trademark","data":{"id":"","tm_name":"","logo_url":""},"before":{},"type":"insert","sinkTable":"dwd_base_trademark"}
        kafkaDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<>(
                        element.getString("sinkTable"),
                        element.getString("data").getBytes()
                );
            }
        }));

        hbaseDS.addSink(new DimSinkFunction());

        //TODO 8.启动任务
        env.execute();

    }
}
