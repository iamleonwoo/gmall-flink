package com.atguigu.ztest;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: MyJdbcUtil
 * Package: com.atguigu.ztest
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/23 1:02
 * @Version 1.0
 */
public class MyJdbcUtil {

    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clazz, boolean toCamel){

        ArrayList<T> resultList = new ArrayList<>();

        PreparedStatement preparedStatement = null;

        try {
            preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet != null){

                T t = clazz.newInstance();

                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);

                    if (toCamel){
                        CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }

                    String value = resultSet.getString(i);

                    BeanUtils.setProperty(t, columnName, value);
                }

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
}
