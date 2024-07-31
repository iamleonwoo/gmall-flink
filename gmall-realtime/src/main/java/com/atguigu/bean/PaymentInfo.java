package com.atguigu.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * ClassName: PaymentInfo
 * Package: com.atguigu.bean
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/23 22:53
 * @Version 1.0
 */
@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}
