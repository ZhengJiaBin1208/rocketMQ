package com.zjb.rocketmq.bean;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @ClassName ProducerBean
 * @Description 生产者Bean
 * @Author zhengjiabin
 * @Date 2023/7/5 14:41
 * @Version 1.0
 **/
@Data
@Slf4j
public class ProducerBean {

    // 生产者producerGroup
    private String producerGroup;

    // 生产者NameServer
    private String namesrvAddr;

    // 生产者topic
    private String topic;

    // 生产者keys
    private String keys;

    // 生产消息数量
    private int producerNum;

    // 生产者消息List
    private List list;







}
