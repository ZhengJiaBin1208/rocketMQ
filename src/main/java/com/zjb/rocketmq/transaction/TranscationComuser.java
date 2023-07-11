package com.zjb.rocketmq.transaction;

import com.zjb.rocketmq.bean.ConsumerBean;
import com.zjb.rocketmq.utils.ConfigUtil;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ClassName TranscationComuser
 * @Description 事务消息-消费者 B 所在B系统 要对B进行+100块钱的操作
 * @Author zhengjiabin
 * @Date 2023/7/11 11:36
 * @Version 1.0
 **/
@Slf4j
public class TranscationComuser {

    static AtomicInteger errcount = new AtomicInteger(0);

    public static void main(String[] args) throws MQClientException {
        ConsumerBean consumerBean = new ConsumerBean();
        consumerBean.setConsumerGroup("TranscationComsuer");
        consumerBean.setNamesrvAddr("127.0.0.1:9876");
        DefaultMQPushConsumer consumer = ConfigUtil.getConsumer(consumerBean);
        consumer.subscribe("TransactionTopic","*");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                try {
                    // todo 开启事务
                    for (MessageExt msg : msgs) {
                        //设置日期格式
                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        log.info("begin");
                        log.info("B服务器本地事务  ,TransactionId: {}",msg.getTransactionId());
                        log.info("update B ...(B账户加100块) : {}",df.format(new Date()));
                        log.info("commit {}",df.format(new Date()));
                    }

                }catch (Exception e){
                    errcount.getAndIncrement();
                    if (errcount.get() > 10){
                        // 这里就需要走消息补偿策略(这里是列举了一种)
                        try {
                            DefaultMQProducer producer = ConfigUtil.getProducer("group_test", "127.0.0.1:9876");
                            producer.start();
                            Message msg = new Message("ErrTranscation" /* Topic */,
                                    "" /* Tag*/,
                                    msgs.get(0).getBody() /* Message body */);
                            // 发送消息到一个Broker
                            SendResult sendResult = producer.send(msg);

                        }catch (Exception e1){
                            e1.printStackTrace();
                        }
                        return null;
                    }
                    e.printStackTrace();
                    log.info("执行本地事务失败，重试消费，尽量确保B处理成功");
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者
        consumer.start();
        log.info("------TranscationComuser Started-----");
    }
}
