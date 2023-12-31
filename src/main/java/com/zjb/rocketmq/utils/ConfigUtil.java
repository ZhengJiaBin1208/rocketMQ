package com.zjb.rocketmq.utils;

import com.zjb.rocketmq.bean.ConsumerBean;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

/**
 * @ClassName ConfigUtil
 * @Description 配置工具类
 * @Author zhengjiabin
 * @Date 2023/7/4 11:12
 * @Version 1.0
 **/
public class ConfigUtil {

    public static DefaultMQProducer getProducer(String producerGroup, String addr){
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        // 设置NameServer的地址
        producer.setNamesrvAddr(addr);
        return producer;
    }

    public static DefaultMQPushConsumer getConsumer(ConsumerBean consumerBean){
        // 实例化消息生产者Producer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerBean.getConsumerGroup());
        // 设置NameServer的地址
        consumer.setNamesrvAddr(consumerBean.getNamesrvAddr());
        return consumer;
    }

}
