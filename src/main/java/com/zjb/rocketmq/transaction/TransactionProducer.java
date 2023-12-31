package com.zjb.rocketmq.transaction;

import com.zjb.rocketmq.bean.ConsumerBean;
import com.zjb.rocketmq.utils.ConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName TransactionProducer
 * @Description
 * 分布式事务场景（用户A向用户B 转100块钱，用户A在A系统，用户B在B系统）
 * 这里TransactionProducer  是A系统模拟用户A 在A系统进行金额扣减
 * @Author zhengjiabin
 * @Date 2023/7/7 17:43
 * @Version 1.0
 **/
@Slf4j
public class TransactionProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 创建事务监听器
        TransactionListener transactionListener = new TransactionListenerImpl();

        // 1.创建一个事务消息发送的生产者(TransactionProducer生产者分组: 针对就是分布式消息)
        TransactionMQProducer producer = new TransactionMQProducer("TransactionProducer");
        producer.setNamesrvAddr("127.0.0.1:9876");
        // 创建线程池(JUC提供的)
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });
        // 设置生产者回查线程池
        producer.setExecutorService(executorService);
        // 生产者设置监听器
        producer.setTransactionListener(transactionListener);
        // 启动消息生产者
        producer.start();
        // 1.发送事务消息(一定要用这个方法,sendMessageInTransaction)
        try {
            Message msg = new Message("TransactionTopic", null, ("用户A向用户B系统转100块").getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.sendMessageInTransaction(msg, null);
            if (sendResult.getSendStatus() == SendStatus.SEND_OK){
                // 设置日期格式
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                log.info("发送半事务消息成功: {}",df.format(new Date()));
            }else {
                log.info("发送半事务消息失败!!!");
                return;
            }
        } catch (MQClientException | UnsupportedEncodingException e) {
            log.info("发送半事务消息失败!!!");
            e.printStackTrace();
        }
        // 启动事务补偿的消费监听
        Listener();
        // 一些长时间等待的业务(比如输入密码,确认等操作):需要通过事务回查来处理
        for (int i = 0; i < 1000; i++){
            Thread.sleep(1000);
        }
        producer.shutdown();
    }

    public static void Listener(){
        try {
        ConsumerBean consumerBean = new ConsumerBean();
        consumerBean.setConsumerGroup("balance_consumer");
        consumerBean.setNamesrvAddr("127.0.0.1:9876");
        DefaultMQPushConsumer consumer = ConfigUtil.getConsumer(consumerBean);
        consumer.subscribe("ErrTranscation","*");
        // 注册回调函数(并发消费模式),处理消息
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    try {
                        for (MessageExt msg : msgs) {
                            // 设置日期格式
                            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            // todo 执行本地事务的回退补偿方案
                            log.info("begin");
                            log.info("update A ...(A账户加100块): {}",df.format(new Date()));
                            log.info("commit {}",df.format(new Date()));
                        }

                    }catch (Exception e){
                        e.printStackTrace();
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                    // 消费成功
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            // 启动消费者
            consumer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }

    }
}
