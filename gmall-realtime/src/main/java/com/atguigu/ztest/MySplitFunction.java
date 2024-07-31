package com.atguigu.ztest;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * ClassName: MySplitFunction
 * Package: com.atguigu.ztest
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/26 21:52
 * @Version 1.0
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class MySplitFunction extends TableFunction<Row> {
    public void eval(String str) {
        for (String s : MyKeywordUtil.splitKeyWord(str)) {
            // use collect(...) to emit a row
            collect(Row.of(s));
        }
    }
}