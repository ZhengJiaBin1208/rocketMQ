package com.zjb.rocketmq.filter;

import com.zjb.rocketmq.utils.ConfigUtil;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * @ClassName SqlFilterProducer
 * @Description sql过滤-消息生产者(加入消息属性)
 * @Author zhengjiabin
 * @Date 2023/7/5 11:00
 * @Version 1.0
 **/
public class SqlFilterProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = ConfigUtil.getProducer("SqlFilterProducer","127.0.0.1:9876");
        producer.start();

        String[] tags = {"TagA", "TagB", "TagC"};
        String topic = "SqlFilterTest";
        String msg = "SqlFilterProducer";

        for (int i = 0; i < 10; i++){
            Message message = new Message(topic, tags[i % tags.length], (msg + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 设置sql过滤属性
            message.putUserProperty("a",String.valueOf(i));
            SendResult sendResult = producer.send(message);
            System.out.println("sendResult: "+sendResult);
        }

        producer.shutdown();
    }
}
