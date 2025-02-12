package com.atguigu.common;

/**
 * ClassName: GmallConfig
 * Package: com.atguigu.common
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/19 17:30
 * @Version 1.0
 */
public class GmallConfig {

    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL210225_REALTIME";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hadoop102:8123/default";

    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

}
