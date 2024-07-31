package com.atguigu.ztest;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * ClassName: MyDimSinkFunction
 * Package: com.atguigu.ztest
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/19 23:31
 * @Version 1.0
 */
public class MyDimSinkFunction extends RichSinkFunction<JSONObject>{

    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //获取插入数据的SQL upsert into db.tn(id,tm_name) values(..,..)
        try {
            String upsertSql = getSql(value.getString("sinkTable"), value.getJSONObject("data"));

            preparedStatement = connection.prepareStatement(upsertSql);

            if ("update".equals(value.getString("type"))){
                String redisKey = "DIM:" + value.getString("sinkTable") + ":"
                        + value.getJSONObject("data").getString("id");
                MyDimUtil.delDimInfo(redisKey);
            }

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

    //upsert into db.t(aa, bb, cc, dd) values('1','2','3','4')
    private String getSql(String sinkTable, JSONObject data) {
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();

        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable
                + "(" + StringUtils.join(columns, ",") + ")"
                + "values('" + StringUtils.join(values, "','") + "')";

        return sql;
    }
}
