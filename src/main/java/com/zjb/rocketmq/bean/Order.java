package com.zjb.rocketmq.bean;

import lombok.Data;
import lombok.ToString;

/**
 * @ClassName Order
 * @Description TODO
 * @Author zhengjiabin
 * @Date 2023/7/6 16:11
 * @Version 1.0
 **/
@Data
@ToString
public class Order {
    // 订单Id
    private long orderId;

    // 订单详细信息
    private String desc;



}
