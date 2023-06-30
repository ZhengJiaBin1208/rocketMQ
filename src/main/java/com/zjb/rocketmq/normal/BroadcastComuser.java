package com.zjb.rocketmq.normal;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @ClassName BroadcastComuser
 * @Description 广播模式消费者
 * @Author zhengjiabin
 * @Date 2023/6/30 16:42
 * @Version 1.0
 **/
public class BroadcastComuser {
    public static void main(String[] args) throws MQClientException {
        // 实例化消息生产者,指定组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("broadcast_group");
        // 指定NameServer地址信息
        consumer.setNamesrvAddr("127.0.0.1:9876");
        // 订阅Topic
        consumer.subscribe("TopicTest", "*");
        // 广播消费模式
        consumer.setMessageModel(MessageModel.BROADCASTING);
        // 注册回调函数(并发消费模式),处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                try {
                    for (MessageExt msg : msgs) {
                        String topic = msg.getTopic();
                        String msgBody = new String(msg.getBody(), "utf-8");
                        String tags = msg.getTags();
                        System.out.println("收到消息：" + " topic :" + topic + " ,tags : " + tags + " ,msg : " + msgBody);
                    }
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    //程序返回 Null或者直接抛出异常，对于RocketMQ来说都是走重试

                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; // 消费成功
            }
        });

        // 启动消费者
        consumer.start();
        // 注销Consumer
        // consumer.shutdown();
        System.out.printf("Consumer Started.%n");
    }
}
