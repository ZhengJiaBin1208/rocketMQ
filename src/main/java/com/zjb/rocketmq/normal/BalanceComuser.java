package com.zjb.rocketmq.normal;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @ClassName BalanceComuser
 * @Description 集群消费
 * @Author zhengjiabin
 * @Date 2023/6/30 10:05
 * @Version 1.0
 **/
public class BalanceComuser {
    public static void main(String[] args) throws MQClientException {
        // 实例化消费者--推模式--订阅模式
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("balance_consumer");
        // 指定Namesrv地址信息.
        consumer.setNamesrvAddr("127.0.0.1:9876");
        // 集群模式消费(默认就是，所以可以不用写)
        consumer.setMessageModel(MessageModel.CLUSTERING);
        // 订阅Topic
        consumer.subscribe("TopicTest","*"); //tag tagA|tagB|tagC
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET); //从最早的偏移量开始消费

        // 注册回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

                for (MessageExt msg : msgs) {
                    String topic = msg.getTopic();
                    try {
                        String msgBody = new String(msg.getBody(),"utf-8");
                        String tags = msg.getTags();
                        System.out.println("接收消息: " + "topic :" + topic + ",tags :" + tags+ ",msg :" + msgBody);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        // 程序返回 Null或者直接抛出异常，对于RocketMQ来说都是走重试
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; // 消费成功
//                return ConsumeConcurrentlyStatus.RECONSUME_LATER; // 让所有消息重试
            }
        });

        // 启动消费者
        consumer.start();
        // 注销Consumer
//        consumer.shutdown();
        System.out.printf("Consumer Started.%n");

    }
}
