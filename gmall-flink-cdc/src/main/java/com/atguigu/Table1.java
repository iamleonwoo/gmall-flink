package com.atguigu;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: Table1
 * Package: com.atguigu
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/21 2:35
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Table1 {
    private String id;
    private String name;
    private Long ts;
}
