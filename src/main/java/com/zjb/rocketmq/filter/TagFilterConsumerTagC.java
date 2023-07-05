package com.zjb.rocketmq.filter;

import com.zjb.rocketmq.bean.ConsumerBean;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

/**
 * @ClassName SqlFilterConsumer
 * @Description tag过滤-消费者
 * @Author zhengjiabin
 * @Date 2023/7/5 16:06
 * @Version 1.0
 **/
public class TagFilterConsumerTagC {
    public static void main(String[] args) throws MQClientException {
        ConsumerBean consumerBean = new ConsumerBean();
        consumerBean.setConsumerGroup("SqlFilterConsumer");
        consumerBean.setNamesrvAddr("127.0.0.1:9876");
        consumerBean.setTopic("TagFilterTest");
        consumerBean.setFilter("TagC");
        consumerBean.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumerBean.setProperty("a");
//        consumerBean.setFilterType(FilterTypeEnum.SQL_TYPE.getType());
        consumerBean.setMessageModel(MessageModel.CLUSTERING);
        FilterConsumer.openConsumer(consumerBean);
    }
}
