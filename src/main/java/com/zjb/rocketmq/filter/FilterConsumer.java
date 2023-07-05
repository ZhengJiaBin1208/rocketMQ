package com.zjb.rocketmq.filter;

import com.zjb.rocketmq.bean.ConsumerBean;
import com.zjb.rocketmq.enums.FilterTypeEnum;
import com.zjb.rocketmq.utils.ConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @ClassName FilterConsumer
 * @Description 消息过滤消费者
 * @Author zhengjiabin
 * @Date 2023/7/5 14:39
 * @Version 1.0
 **/
@Slf4j
public class FilterConsumer {

    public static void openConsumer(ConsumerBean consumerBean) throws MQClientException {

        //获取实例化消息
        DefaultMQPushConsumer consumer = ConfigUtil.getConsumer(consumerBean);

        //集群消费模式
        consumer.setMessageModel(consumerBean.getMessageModel());

        // 过滤类型判断subscribe的值
        if (null != consumerBean.getFilterType() && consumerBean.getFilterType().equals(FilterTypeEnum.SQL_TYPE.getType())){
            consumer.subscribe(consumerBean.getTopic(), MessageSelector.bySql(consumerBean.getFilter()));
        }else {
            consumer.subscribe(consumerBean.getTopic(), consumerBean.getFilter());
        }

        // 消费者偏移量起始位置
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // 开始消费
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                try {
                for (MessageExt msg : msgs) {
                    String topic = msg.getTopic();
                    String msgBody = new String(msg.getBody(), "utf-8");
                    String tags = msg.getTags();
                    if (StringUtils.isNoneBlank(consumerBean.getProperty())) {
                        String property = msg.getProperty(consumerBean.getProperty());
                        log.info("收到的消息: topic: {},tags: {},msg: {},{}: {}",topic,tags,msgBody,consumerBean.getProperty(),property);
                    }else {
                        log.info("收到的消息: topic: {},tags: {},msg: {}",topic,tags,msgBody);
                    }
                }
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        log.info("Consumer Started .......");
    }
}
