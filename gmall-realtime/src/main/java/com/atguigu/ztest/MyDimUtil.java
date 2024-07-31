package com.atguigu.ztest;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * ClassName: MyDimUtil
 * Package: com.atguigu.ztest
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/23 1:25
 * @Version 1.0
 */
public class MyDimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) {

        Jedis jedis = MyRedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfo = jedis.get(redisKey);

        if (dimInfo != null) {
            JSONObject dimInfoJsonobj = JSONObject.parseObject(dimInfo);
            jedis.expire(redisKey, 24 * 60 * 60);
            jedis.close();
            return dimInfoJsonobj;
        }

        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + id + "'";
        List<JSONObject> queryList = MyJdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimInfoJsonobj = queryList.get(0);

        jedis.set(redisKey, dimInfoJsonobj.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        return dimInfoJsonobj;
    }

    public static void delDimInfo(String redisKey){
        Jedis jedis = MyRedisUtil.getJedis();
        jedis.del(redisKey);
        jedis.close();
    }

}
