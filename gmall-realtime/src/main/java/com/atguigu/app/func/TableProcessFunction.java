package com.atguigu.app.func;

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
 * ClassName: TableProcessFunction
 * Package: com.atguigu.app.func
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/19 16:49
 * @Version 1.0
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    //定义属性,状态描述器
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    //定义属性,侧输出流标记
    private OutputTag<JSONObject> outputTag;

    //声明Phoenix连接
    private Connection connection;

    public TableProcessFunction() {
    }

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> outputTag) {
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
        //1.获取广播状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "_" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        //2.过滤字段
        if (tableProcess != null){
            filterColumn(value.getJSONObject("data"), tableProcess.getSinkColumns());

            //3.动态分流
            String sinkType = tableProcess.getSinkType();
            //将去往的表或主题信息放入数据继续向下游传输，以便后续判断将数据发送到kafka的哪个主题或hbase的哪张表
            value.put("sinkTable", tableProcess.getSinkTable());

            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
                //将数据写入侧输出流(HBase)
                ctx.output(outputTag, value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                //将数据写入主流(Kafka)
                out.collect(value);
            }
        } else {
            //脏数据
            System.out.println(key + "不存在！");
        }

    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        //切割指定信息
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            if (!columnList.contains(next.getKey())){
                iterator.remove();
            }
        }
    }

    //广播流: {"database":"","tableName":"table_process","data":{"sourceTable":"","operateType":"",...,},"before":{},"type":"insert"}
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        //1.获取并解析数据
        JSONObject jsonObject = JSONObject.parseObject(value);
        TableProcess tableProcess = JSONObject.parseObject(jsonObject.getString("data"), TableProcess.class);

        //2.校验表是否存在，如果不存在则建表
        String sinkType = tableProcess.getOperateType();
        String type = jsonObject.getString("type");
        if ("insert".equals(type) && TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
            createHbaseTable(
                    tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
            );
        }

        //3.写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "_" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);

    }

    //建表语句：create table if not exists db.t(id varchar primary key,tm_name varchar) ...
    private void createHbaseTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //处理字段
        if (sinkPk == null){
            sinkPk = "id";
        }
        if (sinkExtend == null){
            sinkExtend = "";
        }

        try {
            //获取建表语句
            StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");

            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                //判断是否为主键
                if (sinkPk.equals(column)) {
                    createTableSql.append(column).append(" varchar primary key");
                } else {
                    createTableSql.append(column).append(" varchar");
                }
                //判断如果不是最后一个字段加上逗号
                if (i < columns.length - 1) {
                    createTableSql.append(",");
                }
            }

            createTableSql.append(")")
                    .append(sinkExtend);

            System.out.println(createTableSql);

            //预编译SQL并赋值
            PreparedStatement preparedStatement = connection.prepareStatement(createTableSql.toString());

            //执行SQL语句并提交
            preparedStatement.execute();
            connection.commit();

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表" + sinkTable + "失败！");
        }
    }
}
