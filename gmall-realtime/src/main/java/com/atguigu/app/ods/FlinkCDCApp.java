package com.atguigu.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.MyStringDeserializationSchema;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * ClassName: FlinkCDCApp
 * Package: com.atguigu.app
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/18 16:37
 * @Version 1.0
 */
public class FlinkCDCApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //开启CK（生产环境中记得开启！）
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //......
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck/"));

        //TODO 2.通过FlinkCDC构建Source
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-210225-flink")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyStringDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        //TODO 3.将数据写入Kafka
        dataStreamSource.print();
        dataStreamSource.addSink(MyKafkaUtil.getFlinkKafkaProducer("ods_base_db"));

        //TODO 4.启动任务
        env.execute();

    }
}
