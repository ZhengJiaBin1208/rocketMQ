package com.zjb.rocketmq.normal;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * @ClassName SyncProducer
 * @Description 同步发送:比较可靠的场景，消息通知，短信通知
 * @Author zhengjiabin
 * @Date 2023/6/29 18:11
 * @Version 1.0
 **/
public class SyncProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, MQBrokerException, RemotingException, InterruptedException {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("group_test");
        // 设置NameServer的地址
        producer.setNamesrvAddr("127.0.0.1:9876");
        // 启动Producer实例
        producer.start();
        // 发送10条消息
        for (int i = 0; i < 10; i++){
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message(
                    "TopicTest" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            // 发送消息到一个Broker
            SendResult sendResult = producer.send(msg); // 这里会堵塞 比如发一个文件(2m)
            System.out.printf("%s%n",sendResult);
        }
        // 如果不在发送消息，关闭Producer实例。
        producer.shutdown();

    }

}
