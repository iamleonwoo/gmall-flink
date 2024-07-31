package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * ClassName: DimUtil
 * Package: com.atguigu.utils
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/22 21:34
 * @Version 1.0
 */
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id){

        //查询Redis中的数据
        String redisKey = "DIM: " + tableName + ":" + id;
        Jedis jedis = RedisUtil.getJedis();
        String dimInfo = jedis.get(redisKey);

        if (dimInfo != null){
            JSONObject dimInfoJsonObj = JSONObject.parseObject(dimInfo);
            //重置过期时间
            jedis.expire(redisKey, 24 * 60 *60);
            jedis.close();
            return dimInfoJsonObj;
        }

        //拼接SQL
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + id + "'";
        System.out.println(querySql);
        //查询
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);

        //将查询到的数据写入Redis缓存并设置过期时间
        JSONObject dimInfoJsonObj = queryList.get(0);
        jedis.set(redisKey, dimInfoJsonObj.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        //返回结果数据
        return dimInfoJsonObj;

    }

    public static void delDimInfo(String redisKey){
        //获取Redis连接
        Jedis jedis = RedisUtil.getJedis();
        //删除数据
        jedis.del(redisKey);
        //归还连接
        jedis.close();
    }

    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        JSONObject info = getDimInfo(connection, "DIM_BASE_TRADEMARK", "10");
        System.out.println(info);
        connection.close();
    }

}
