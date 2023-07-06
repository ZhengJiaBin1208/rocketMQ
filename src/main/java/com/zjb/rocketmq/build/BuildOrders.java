package com.zjb.rocketmq.build;

import com.zjb.rocketmq.bean.Order;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName buildOrders
 * @Description 创建订单
 * @Author zhengjiabin
 * @Date 2023/7/6 16:14
 * @Version 1.0
 **/
public class BuildOrders {

    /**
     * @author zhengjiabin
     * @description
     * 生成模拟订单数据 3个订单 消息中就会有12条(每个订单4个状态)
     * 每个订单 创建->付款->推送->完成 (每个步骤都有一个消息)
     * @date 2023/7/6 16:15
     * @return java.util.List<com.zjb.rocketmq.bean.Order>
     **/
    public List<Order> buildOrders(){
        ArrayList<Order> orderList = new ArrayList<>();
        Order order = new Order();

        order.setOrderId(001);
        order.setDesc("创建");
        orderList.add(order);

        order = new Order();
        order.setOrderId(002);
        order.setDesc("创建");
        orderList.add(order);

        order = new Order();
        order.setOrderId(001);
        order.setDesc("付款");
        orderList.add(order);

        order = new Order();
        order.setOrderId(003);
        order.setDesc("创建");
        orderList.add(order);

        order = new Order();
        order.setOrderId(002);
        order.setDesc("付款");
        orderList.add(order);

        order = new Order();
        order.setOrderId(003);
        order.setDesc("付款");
        orderList.add(order);

        order = new Order();
        order.setOrderId(002);
        order.setDesc("推送");
        orderList.add(order);

        order = new Order();
        order.setOrderId(003);
        order.setDesc("推送");
        orderList.add(order);

        order = new Order();
        order.setOrderId(002);
        order.setDesc("完成");
        orderList.add(order);

        order = new Order();
        order.setOrderId(001);
        order.setDesc("推送");
        orderList.add(order);

        order = new Order();
        order.setOrderId(003);
        order.setDesc("完成");
        orderList.add(order);

        order = new Order();
        order.setOrderId(001);
        order.setDesc("完成");
        orderList.add(order);


        return orderList;
    }
}
