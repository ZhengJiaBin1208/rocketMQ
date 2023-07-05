package com.zjb.rocketmq.bean;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

/**
 * @ClassName Consumer
 * @Description TODO
 * @Author zhengjiabin
 * @Date 2023/7/5 14:41
 * @Version 1.0
 **/
@Data
@Slf4j
public class ConsumerBean {

    // 消费者consumerGroup
    private String consumerGroup;

    // 消费者NameServer
    private String namesrvAddr;

    // 消费模式
    private MessageModel messageModel;

    // 过滤条件
    private String filter;

    // 过滤类型
    private String filterType;

    // 消费者偏移量位置
    private ConsumeFromWhere consumeFromWhere;

    // 消费者topic
    private String topic;

    // 消费者Property
    private String property;







}
