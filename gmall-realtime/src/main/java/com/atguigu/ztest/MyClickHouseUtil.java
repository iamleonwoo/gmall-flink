package com.atguigu.ztest;

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
 * ClassName: MyClickHouseUtil
 * Package: com.atguigu.ztest
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/25 23:11
 * @Version 1.0
 */
public class MyClickHouseUtil {

    public static <T> SinkFunction<T> getSink(String sql){

        return JdbcSink.sink(
                sql,
                (JdbcStatementBuilder<T>) (preparedStatement, t) -> {

                    Field[] fields = t.getClass().getDeclaredFields();

                    int skipOffset = 0;
                    for (int i = 0; i < fields.length; i++) {

                        Field field = fields[i];

                        field.setAccessible(true);

                        MyTransientSink annotation = field.getAnnotation(MyTransientSink.class);

                        if (annotation == null){

                            Object value = null;
                            try {
                                value = field.get(t);
                            } catch (IllegalAccessException e) {
                                throw new RuntimeException(e);
                            }

                            preparedStatement.setObject(i + 1 - skipOffset, value);

                        } else {
                            skipOffset++;
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
