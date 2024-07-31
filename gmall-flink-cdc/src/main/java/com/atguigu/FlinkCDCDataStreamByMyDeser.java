package com.atguigu;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * ClassName: FlinkCDCDataStreamByMyDeser
 * Package: com.atguigu
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/17 23:47
 * @Version 1.0
 */
public class FlinkCDCDataStreamByMyDeser {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.通过FlinkCDC构建Source
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-210225-flink")
                .tableList("gmall-210225-flink.base_trademark")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyStringDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        //TODO 3.打印数据
        dataStreamSource.print();

        //TODO 4.启动任务
        env.execute();

    }

    //{
    // "database":"",
    // "tableName":"",
    // "data":{"id":"1001","tm_name","atguigu"....},
    // "before":{"id":"1001","tm_name","atguigu"....},
    // "type":"update",
    // "ts":141564651515
    // }
    private static class MyStringDeserializationSchema implements DebeziumDeserializationSchema<String> {

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

            //获取数据库名称&表名称
            String topic = sourceRecord.topic();
            String[] fields = topic.split("\\.");
            String database = fields[1];
            String tableName = fields[2];

            //获取数据
            Struct value = (Struct) sourceRecord.value();

            //Before数据
            Struct before = (Struct) value.get("before");
            JSONObject beforeJson = new JSONObject();
            if (before != null){ //insert数据，则before为null
                Schema beforeSchema = before.schema();
                List<Field> fieldList = beforeSchema.fields();

                for (Field field : fieldList) {
                    Object beforeValue = before.get(field);
                    beforeJson.put(field.name(), beforeValue);
                }
            }

            //After数据
            Struct after = value.getStruct("after");
            JSONObject afterJson = new JSONObject();
            if (after != null){ //delete数据，则after为null
                Schema afterSchema = after.schema();
                List<Field> fieldList = afterSchema.fields();

                for (Field field : fieldList) {
                    Object afterValue = after.get(field);
                    afterJson.put(field.name(), afterValue);
                }
            }

            //获取操作类型 CREATE UPDATE DELETE
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            String type = operation.toString().toLowerCase();
            if ("create".equals(type)) {
                type = "insert";
            }

            //构建结果对象
            JSONObject result = new JSONObject();

            //封装数据
            result.put("database", database);
            result.put("tableName", tableName);
            result.put("data", afterJson);
            result.put("before", beforeJson);
            result.put("type", type);

            //输出封装好的数据
            collector.collect(result.toJSONString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
