package com.atguigu.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * ClassName: LoggerController
 * Package: com.atguigu.gmalllogger.controller
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/16 23:55
 * @Version 1.0
 */

//@Controller
//@RestController  = @Controller+@ResponseBody
@RestController //表示返回普通对象而不是页面
@Slf4j
public class LoggerController {

//    Logger logger = LoggerFactory.getLogger(LoggerController.class);
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/test")
//    @ResponseBody
    public String test1(){
        System.out.println("111111");
        return "success1";
    }

    @RequestMapping("/test2")
    public String test2(@RequestParam("name") String nm,
                        @RequestParam(value = "age", defaultValue = "18") int age){
        System.out.println(nm + ":" + age);
        return "success2";
    }

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr) {

        //将日志数据打印到控制台&写入日志文件
        log.info(jsonStr);

        //将数据写入Kafka
        kafkaTemplate.send("ods_base_log", jsonStr);

        return "success";
    }

}
