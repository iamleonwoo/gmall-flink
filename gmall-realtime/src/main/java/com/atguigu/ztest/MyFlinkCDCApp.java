package com.atguigu.ztest;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: MyTest
 * Package: com.atguigu.ztest
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/18 17:33
 * @Version 1.0
 */
public class MyFlinkCDCApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //......
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck/"));

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-210225-flink")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyStringDeserialization())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        dataStreamSource.print();

        dataStreamSource.addSink(MyKafkaTool.getFlinkKafkaProducer("ods_base_db"));

        env.execute();


    }
}
