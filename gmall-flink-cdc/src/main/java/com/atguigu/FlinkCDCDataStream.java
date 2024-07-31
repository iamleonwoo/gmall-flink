package com.atguigu;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: FlinkCDCDataStream
 * Package: com.atguigu
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/17 21:02
 * @Version 1.0
 */
public class FlinkCDCDataStream {
    public static void main(String[] args) throws Exception {

        //设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //开启CK
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);

        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/20240417"));

        //TODO 2.创建Flink-MySQL-CDC的Source
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-210225-flink")
                .tableList("gmall-210225-flink.base_trademark") //指定的时候需要使用"db.table"的方式，否则会读取指定数据库下所有表的数据变动
                .startupOptions(StartupOptions.initial())
//                .startupOptions(StartupOptions.earliest())
//                .startupOptions(StartupOptions.latest())
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        //TODO 3.使用CDC Source从MySQL读取数据
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        //TODO 4.打印数据
        dataStreamSource.print();

        //TODO 5.执行任务
        env.execute();


    }
}
