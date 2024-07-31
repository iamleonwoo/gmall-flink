package com.atguigu.ztest;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * ClassName: MyStringDeserialization
 * Package: com.atguigu.ztest
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/18 17:43
 * @Version 1.0
 */
public class MyStringDeserialization implements DebeziumDeserializationSchema<String> {

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