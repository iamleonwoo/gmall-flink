package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * ClassName: DimAsyncFunction
 * Package: com.atguigu.app.func
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/23 21:50
 * @Version 1.0
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T>{

    //声明线程池和Phoenix连接
    private ThreadPoolExecutor threadPoolExecutor;
    private Connection connection;

    //定义属性
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    public abstract String getId(T input);
    public abstract void join(T input, JSONObject dimInfo) throws ParseException;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池和Phoenix连接
        threadPoolExecutor = ThreadPoolUtil.getInstance();
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(
                new Runnable() {
                    @SneakyThrows
                    @Override
                    public void run() {

                        //提取查询维度的id
                        String id = getId(input);

                        //查询维度
                        JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);

                        //补充维度信息
                        join(input, dimInfo);

                        //将关联好维度的数据输出到流中
                        resultFuture.complete(Collections.singleton(input));

                    }
                }
        );
    }


    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("Timeout: " + input);
    }
}