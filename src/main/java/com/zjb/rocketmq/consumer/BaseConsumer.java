package com.zjb.rocketmq.consumer;

import com.zjb.rocketmq.bean.ConsumerBean;
import com.zjb.rocketmq.enums.FilterTypeEnum;
import com.zjb.rocketmq.utils.ConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName FilterConsumer
 * @Description 消费者基础类
 * @Author zhengjiabin
 * @Date 2023/7/5 14:39
 * @Version 1.0
 **/
@Slf4j
public class BaseConsumer {

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

        if (consumerBean.getMsgType().equals("1")){

            consumer.registerMessageListener(new MessageListenerOrderly() {
                Random random = new Random();
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                    context.setAutoCommit(true);
                    for (MessageExt msg : msgs) {
                        // 可以看到每个queue有唯一的consume线程来消费, 订单对每个queue(分区)有序
                        System.out.println("consumeThread=" + Thread.currentThread().getName()
                                + ",queueId=" + msg.getQueueId() + ", content:" + new String(msg.getBody()));
                    }
                    try {
                        //模拟业务逻辑处理中...
                        TimeUnit.MILLISECONDS.sleep(random.nextInt(300));
                    } catch (Exception e) {
                        e.printStackTrace();
                        //这个点要注意：意思是先等一会，一会儿再处理这批消息，而不是放到重试队列里
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            });
        }else{


        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                try {
                for (MessageExt msg : msgs) {
                    if (consumerBean.getMsgType().equals("filter")){
                        String topic = msg.getTopic();
                        String msgBody = new String(msg.getBody(), "utf-8");
                        String tags = msg.getTags();
                        if (StringUtils.isNoneBlank(consumerBean.getProperty())) {
                            String property = msg.getProperty(consumerBean.getProperty());
                            log.info("收到的消息: topic: {},tags: {},msg: {},{}: {}",topic,tags,msgBody,consumerBean.getProperty(),property);
                        }else {
                            log.info("收到的消息: topic: {},tags: {},msg: {}",topic,tags,msgBody);
                        }
                    } else if (consumerBean.getMsgType().equals("scheduled")) {
                        log.info("Receive message [msgId = {} ] ({}-{}) ms later",msg.getMsgId(),msg.getStoreTimestamp(),msg.getBornTimestamp());
                    }

                }
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        }
        consumer.start();
        log.info("Consumer Started .......");
    }
}
