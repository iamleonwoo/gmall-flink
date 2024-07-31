package com.atguigu.ztest;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
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
 * ClassName: MyBaseDbApp
 * Package: com.atguigu.ztest
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/19 21:22
 * @Version 1.0
 */
public class MyBaseDbApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //......
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));

        String topic = "ods_base_db";
        String groupId = "MyBaseDbApp2024";
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaTool.getFlinkKafkaConsumer(topic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStreamSource.map(value -> JSONObject.parseObject(value))
                .filter(value -> {
                    String type = value.getString("type");
                    return !"delete".equals(type);
                });

        DebeziumSourceFunction<String> tableProcessStrSourceFunc = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("12356")
                .databaseList("gmall-210225-realtime")
                .tableList("gmall-210225-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyStringDeserialization())
                .build();

        DataStreamSource<String> tableProcessStrDS = env.addSource(tableProcessStrSourceFunc);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);

        //主流: {"database":"","tableName":"base_trademark","data":{"id":"","tm_name":"","logo_url":""},"before":{},"type":"insert"}
        //广播流: {"database":"","tableName":"table_process","data":{"sourceTable":"","operateType":"",...,},"before":{},"type":"insert"}
        BroadcastConnectedStream<JSONObject, String> broadcastConnectedStream = jsonObjDS.connect(broadcastStream);

        OutputTag<JSONObject> hbaseTag = new OutputTag<>("hbase");
        SingleOutputStreamOperator<JSONObject> kafkaDS = broadcastConnectedStream.process(new MyTableProcessFunction(mapStateDescriptor, hbaseTag));

        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);

        kafkaDS.addSink(MyKafkaTool.getFlinkKafkaProducer(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                        return new ProducerRecord<>(
                                element.getString("sinkTable"),
                                element.getString("data").getBytes()
                        );
                    }
                }
        ));
        hbaseDS.addSink(new MyDimSinkFunction());

        env.execute();
    }
}
