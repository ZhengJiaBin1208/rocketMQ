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
 * @ClassName TagFilterProducer
 * @Description tag过滤-生产者
 * @Author zhengjiabin
 * @Date 2023/7/5 14:24
 * @Version 1.0
 **/
public class TagFilterProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = ConfigUtil.getProducer("TagFilterProducer","127.0.0.1:9876");
        producer.start();
        // 设定三种标签
        String[] tags = {"TagA","TagB","TagC"};
        String topic = "TagFilterTest";
        String message = "TagFilterProducer";

        for (int i = 0; i < 3; i++){
            Message msg = new Message(topic, tags[i % tags.length], message.getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(msg);
            System.out.println("sendResult: "+ sendResult);
        }

        producer.shutdown();
    }
}
