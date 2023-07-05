package com.zjb.rocketmq.filter;

import com.zjb.rocketmq.bean.ConsumerBean;
import com.zjb.rocketmq.enums.FilterTypeEnum;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

/**
 * @ClassName SqlFilterConsumer
 * @Description sql过滤消费者
 * @Author zhengjiabin
 * @Date 2023/7/5 16:06
 * @Version 1.0
 **/
public class SqlFilterConsumer {
    public static void main(String[] args) throws MQClientException {
        ConsumerBean consumerBean = new ConsumerBean();
        consumerBean.setConsumerGroup("SqlFilterConsumer");
        consumerBean.setNamesrvAddr("127.0.0.1:9876");
        consumerBean.setTopic("SqlFilterTest");
        consumerBean.setFilter("(TAGS is not null and TAGS in ('TagA','TagB')) and (a is not null and a between 0 and 3)");
        consumerBean.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumerBean.setProperty("a");
        consumerBean.setFilterType(FilterTypeEnum.SQL_TYPE.getType());
        consumerBean.setMessageModel(MessageModel.CLUSTERING);
        FilterConsumer.openConsumer(consumerBean);
    }
}
