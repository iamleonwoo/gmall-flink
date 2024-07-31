package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * ClassName: ClickHouseUtil
 * Package: com.atguigu.utils
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/25 22:10
 * @Version 1.0
 */
public class ClickHouseUtil {

    public static <T> SinkFunction<T> getSink(String sql) {
        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        //获取所有的属性名称
                        Field[] fields = t.getClass().getDeclaredFields();

                        //遍历字段信息,获取数据内容并给preparedStatement赋值
                        int skipOffset = 0;
                        for (int i = 0; i < fields.length; i++) {

                            //获取字段名
                            Field field = fields[i];

                            //设置私有属性值可访问
                            field.setAccessible(true);

                            //获取该字段上的注解
                            TransientSink annotation = field.getAnnotation(TransientSink.class);

                            if (annotation == null){

                                Object value = null;
                                try {
                                    //获取值
                                    value = field.get(t);
                                } catch (IllegalAccessException e) {
                                    e.printStackTrace();
                                }

                                //给preparedStatement中的"?"赋值
                                preparedStatement.setObject(i + 1 - skipOffset, value);

                            } else {
                                skipOffset++;
                            }
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
                );
    }

}
