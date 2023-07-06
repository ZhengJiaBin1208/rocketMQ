package com.zjb.rocketmq.producer;

import com.zjb.rocketmq.bean.Order;
import com.zjb.rocketmq.bean.ProducerBean;
import com.zjb.rocketmq.build.BuildOrders;
import com.zjb.rocketmq.ordermessage.ProducerInOrder;
import com.zjb.rocketmq.utils.ConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName BaseProducer
 * @Description 生产者基础类
 * @Author zhengjiabin
 * @Date 2023/7/6 16:08
 * @Version 1.0
 **/
@Slf4j
public class BaseProducer {

    public static void openProducer(ProducerBean producerBean,Class c) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = ConfigUtil.getProducer(producerBean.getProducerGroup(),producerBean.getNamesrvAddr());
        producer.start();
        List<Order> orderList = new ArrayList<>();
        for (int i = 0; i < producerBean.getProducerNum(); i++) {
            String body = "";
            if (c.equals(ProducerInOrder.class)){
                // 订单列表(整体上无序，但是针对某个订单号 是有序的 12条消息。3个订单<每个订单4个状态>)
                orderList = producerBean.getList();
                body = orderList.get(i).toString();

            Message msg = new Message(producerBean.getTopic(), null, producerBean.getKeys() + i, body.getBytes());
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Long id = (Long) arg;  //根据订单id选择发送queue
                    long index = id % mqs.size();
                    return mqs.get((int) index);
                }
            }, orderList.get(i).getOrderId());//订单id
                log.info("SendResult status: {}, queueId: {}, body: {}",
                    sendResult.getSendStatus(),
                    sendResult.getMessageQueue().getQueueId(),
                    body);
            }
        }
        producer.shutdown();
    }
}
