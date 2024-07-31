package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.RedisUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * ClassName: DimSinkFunction
 * Package: com.atguigu.app.func
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/19 20:16
 * @Version 1.0
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    //声明Phoenix连接
    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //value:{"database":"","tableName":"base_trademark","data":{"id":"","tm_name":""},"before":{},"type":"insert","sinkTable":"dim_base_trademark"}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        try {
            String upsertSql = genUpsertSql(value.getString("sinkTable"), value.getJSONObject("data"));
            System.out.println(upsertSql);

            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            //判断当前维度数据是否为更新操作，如果是的话，那就需要先删除Redis中对应的旧数据
            if ("update".equals(value.getString("type"))){
                String redisKey = "DIM: " + value.getString("sinkTable").toUpperCase() +":"
                        + value.getJSONObject("data").getString("id");
                DimUtil.delDimInfo(redisKey);
            }

            //执行SQL语句并提交
            preparedStatement.execute();
            connection.commit();

        } catch (SQLException e) {
            System.out.println("插入维度数据" + value.getString("data") + "失败！");
        }
    }

    @Override
    public void close() throws Exception {
        preparedStatement.close();
        connection.close();
    }

    //upsert into db.tn(id,tm_name,aa,bb) values('..','..','...','...')
    //data:{"id":"1001","tm_name":"atguigu"}
    private String genUpsertSql(String sinkTable, JSONObject data) {
        //取出字段
        Set<String> columns = data.keySet();
        //取出值
        Collection<Object> values = data.values();

        String upsertSql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable
                + "(" + StringUtils.join(columns, ",") + ")"
                + "values('" + StringUtils.join(values, "','") + "')";

        return upsertSql;
    }

}
