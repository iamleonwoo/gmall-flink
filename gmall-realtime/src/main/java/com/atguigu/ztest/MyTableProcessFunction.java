package com.atguigu.ztest;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * ClassName: MyTableProcessFunction
 * Package: com.atguigu.ztest
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/19 22:08
 * @Version 1.0
 */
public class MyTableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject>{

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private OutputTag<JSONObject> outputTag;
    private Connection connection;

    public MyTableProcessFunction() {
    }

    public MyTableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> outputTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

    }

    //主流: {"database":"","tableName":"base_trademark","data":{"id":"","tm_name":"","logo_url":""},"before":{},"type":"insert"}
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        String key = value.getString("tableName") + "_" + value.getString("type");

        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null){
            filterColumn(value.getJSONObject("data"), tableProcess.getSinkColumns());
            value.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                ctx.output(outputTag, value);
            }
        } else {
            System.out.println(key + "不存在！");
        }


    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] column = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(column);

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            if (!columnList.contains(next)){
                iterator.remove();
            }
        }
    }

    //广播流: {"database":"","tableName":"table_process","data":{"sourceTable":"","operateType":"",...,},"before":{},"type":"insert"}
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        TableProcess tableProcess = JSONObject.parseObject(jsonObject.getString("data"), TableProcess.class);

        String type = jsonObject.getString("type");
        String sinkType = tableProcess.getSinkType();
        if ("insert".equals(type) && TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
            createHbaseTable(
                    tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
            );
        }

        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "_" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);

    }

    //建表语句: create table if not exists db.t(id varchar primary key, tm_name varchar) ...
    private void createHbaseTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        if (sinkPk == null){
            sinkPk = "id";
        }

        if (sinkExtend == null){
            sinkExtend = "";
        }

        StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        String[] columns = sinkColumns.split(",");
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            if (sinkPk.equals(column)){
                createTableSql.append(column)
                        .append(" varchar primary key");
            } else {
                createTableSql.append(column)
                        .append(" varchar");
            }
            if (i < columns.length - 1){
                createTableSql.append(",");
            }
        }

        createTableSql.append(")").append(sinkExtend);

        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(createTableSql.toString());
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表" + sinkTable + "失败！");
        }
    }
}
