package com.atguigu.ztest;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: MyKeywordUtil
 * Package: com.atguigu.ztest
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/26 21:53
 * @Version 1.0
 */
public class MyKeywordUtil {

    public static List<String> splitKeyWord(String str) {

        ArrayList<String> result = new ArrayList<>();

        StringReader reader = new StringReader(str);

        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        try {
            Lexeme next = ikSegmenter.next();

            while (next != null){

                String keyword = next.getLexemeText();
                result.add(keyword);

                next = ikSegmenter.next();

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;

    }

    public static void main(String[] args) {
        List<String> keyWords = splitKeyWord("Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待");
        System.out.println(keyWords);
    }
}
