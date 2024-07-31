package com.atguigu.app.func;

import com.atguigu.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * ClassName: SplitFunction
 * Package: com.atguigu.app.func
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/26 20:38
 * @Version 1.0
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String str) {
        List<String> keyWords = KeywordUtil.splitKeyWord(str);
        for (String keyWord : keyWords) {
            // use collect(...) to emit a row
            collect(Row.of(keyWord));
        }
    }
}