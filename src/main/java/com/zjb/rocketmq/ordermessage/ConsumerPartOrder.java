package com.zjb.rocketmq.ordermessage;

import com.zjb.rocketmq.bean.ConsumerBean;
import com.zjb.rocketmq.consumer.BaseConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

/**
 * @ClassName ConsumerPartOrder
 * @Description 订单顺序消费消息
 * @Author zhengjiabin
 * @Date 2023/7/6 18:14
 * @Version 1.0
 **/
public class ConsumerPartOrder {
    public static void main(String[] args) throws MQClientException {
        ConsumerBean consumerBean = new ConsumerBean();
        consumerBean.setConsumerGroup("OrderConsumer");
        consumerBean.setMsgType("1");
        consumerBean.setNamesrvAddr("127.0.0.1:9876");
        consumerBean.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumerBean.setTopic("OrderTopic");
        consumerBean.setFilter("*");
        BaseConsumer.openConsumer(consumerBean);
    }
}
