package com.zjb.rocketmq.normal;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @ClassName ListenerOrderComuser
 * @Description 集群消费
 * @Author zhengjiabin
 * @Date 2023/6/30 15:46
 * @Version 1.0
 **/
public class ListenerOrderComuser {
    public static void main(String[] args) throws MQClientException {
        // 实例化消费者--推模式--订阅模式
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_consumer");
        // 指定NameServer地址
        consumer.setNamesrvAddr("127.0.0.1:9876");
        // 集群消费模式(默认就是，可以不用写)
        consumer.setMessageModel(MessageModel.CLUSTERING);
        // 订阅Topic
        consumer.subscribe("TopicTest", "*"); //tag tagA|tagB|tagC
        // 这里是消费者从哪里开始
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET); // 从最早的偏移量开始消费
        consumer.setSuspendCurrentQueueTimeMillis(5 * 1000); // 返回SUSPEND_CURRENT_QUEUE_A_MOMENT等待毫秒数，5.0版本默认是 1000
        // 注册回调函数(顺序消费模式),处理消息 这个写法,能够一定100% 保证消息的顺序呢?
        // MessageListenerOrderly 只允许一个线程消费一个队列的消息
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                try {
                    for (MessageExt msg : msgs) {
                        String topic = msg.getTopic();
                        String msgBody = new String(msg.getBody(), "utf-8");
                        String tags = msg.getTags();
                        System.out.println("接收消息: " + "topic :" + topic + ",tags :" + tags+ ",msg :" + msgBody);
                    }
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

    }
}
