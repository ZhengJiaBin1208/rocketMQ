package com.zjb.rocketmq.batch;

import com.zjb.rocketmq.utils.ConfigUtil;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName SplitBatchProducer
 * @Description 批量消息-超过4M-生产者
 * @Author zhengjiabin
 * @Date 2023/7/4 11:10
 * @Version 1.0
 **/
public class SplitBatchProducer {

    public static void main(String[] args) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        // 获取消息生产者Producer
        DefaultMQProducer producer = ConfigUtil.getProducer("BatchProducer", "127.0.0.1:9876");
        // 启动Producer实例
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
            System.out.println("启动Producer实例失败");
        }
        String topic = "BatchTest";
        String tags = "Tag";
        String keys = "OrderID";
        String message = "SplitBatchProducer";
        // 使用List组装
        List<Message> messages = new ArrayList<>(1000 * 1000);
        // 100W元素的数组
        for (int i = 0; i < 1000 * 1000; i++){
            messages.add(new Message(topic,tags,keys + i,(message + i).getBytes()));
        }

        // 把大的消息分裂成若干个小的消息(1M左右)
        ListSplitter splitter = new ListSplitter(messages);
        while (splitter.hasNext()) {
            List<Message> listItem = splitter.next();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("sleep失败");
            }
            producer.send(listItem);
        }
        // 如果不发送消息,关闭Producer
        producer.shutdown();
    }
}
