package com.zjb.rocketmq.ordermessage;

import com.zjb.rocketmq.bean.Order;
import com.zjb.rocketmq.bean.ProducerBean;
import com.zjb.rocketmq.build.BuildOrders;
import com.zjb.rocketmq.producer.BaseProducer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

/**
 * @ClassName ProducerInOrder
 * @Description 订单顺序生产消息
 * @Author zhengjiabin
 * @Date 2023/7/6 17:50
 * @Version 1.0
 **/
public class ProducerInOrder {
    public static void main(String[] args) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        ProducerBean producerBean = new ProducerBean();
        producerBean.setProducerGroup("OrderProducer");
        producerBean.setNamesrvAddr("127.0.0.1:9876");
        List<Order> orderList = new BuildOrders().buildOrders();
        producerBean.setList(orderList);
        producerBean.setProducerNum(orderList.size());
        producerBean.setKeys("KEY");
        producerBean.setTopic("OrderTopic");
        BaseProducer.openProducer(producerBean, ProducerInOrder.class);
    }
}
