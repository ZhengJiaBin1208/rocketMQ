package com.zjb.rocketmq.batch;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName BatchProducer
 * @Description 批量消息-生产者 list不要超过4m
 * @Author zhengjiabin
 * @Date 2023/6/30 17:54
 * @Version 1.0
 **/
public class BatchProducer {
    public static void main(String[] args) {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("BatchProducer");
        // 设置NameServer的地址
        producer.setNamesrvAddr("127.0.0.1:9876");
        // 启动Producer实例
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        String topic = "BatchTest";
        List<Message> messages = new ArrayList<>();
        /**
         * 1.这里必须是相同的topic,否则会报错
         * 2.这里list的数据不能超过4M,否则会报错
         * 3.这里list的数据只会进入topic的一个queue中
         */
        int sendCount = 10000;
        for (int i = 0; i < sendCount; i++) {
            messages.add(new Message(topic, "", null, ("BatchProducer Test" + i).getBytes()));
        }
        try {
            producer.send(messages);
        } catch (Exception e) {
            e.printStackTrace();
            producer.shutdown();
        }
        // 如果不在发送消息，关闭Producer实例。 RocketMQ5新特性 1:35:59
        producer.shutdown();

    }
}
