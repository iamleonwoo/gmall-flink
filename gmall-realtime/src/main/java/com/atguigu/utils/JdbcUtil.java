package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: JdbcUtil
 * Package: com.atguigu.utils
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/22 20:06
 * @Version 1.0
 */
public class JdbcUtil {

    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clazz, boolean toCamel){
        //创建结果集合
        ArrayList<T> resultList = new ArrayList<>();

        PreparedStatement preparedStatement = null;

        try {
            //预编译SQL
            preparedStatement = connection.prepareStatement(sql);
            //执行查询
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            //遍历查询结果集,封装对象放入list集合
            while (resultSet.next()){ //遍历每一行

                //创建泛型对象
                T t = clazz.newInstance();

                for (int i = 1; i <= columnCount; i++) { //遍历每一列
                    //取出列名
                    String columnName = metaData.getColumnName(i);

                    //转换列名格式
                    if (toCamel){
                        CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }

                    //取出值
                    String value = resultSet.getString(i);

                    //给泛型对象赋值
                    BeanUtils.setProperty(t, columnName, value);
                }
                //将泛型对象添加至结果集合
                resultList.add(t);
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }


        return resultList;
    }

    //测试
    public static void main(String[] args) throws Exception {

//        System.out.println(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, "TM_NAME"));

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        String sql = "select * from GMALL210225_REALTIME.DIM_BASE_TRADEMARK where id='10'";
        List<JSONObject> list = queryList(connection, sql, JSONObject.class, false);
        System.out.println(list);

        connection.close();
    }

}
