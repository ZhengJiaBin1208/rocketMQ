package com.zjb.rocketmq.transaction;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @ClassName TransactionListenerImpl
 * @Description 事务监听
 * @Author zhengjiabin
 * @Date 2023/7/7 17:24
 * @Version 1.0
 **/
@Slf4j
public class TransactionListenerImpl implements TransactionListener {


    /**
     * When send transactional prepare(half) message succeed, this method will be invoked to execute local transaction.
     * 当发送事务准备(半)消息成功时，将调用此方法执行本地事务。
     *
     * @param msg Half(prepare) message
     * @param arg Custom business parameter
     * @return Transaction state
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        // 设置日期格式
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // todo 执行本地事务 update A...
        log.info("begin 本地事务, A服务器的TransactionId: {}",msg.getTransactionId());
        log.info("update A ...(A账户减100块): {}",df.format(new Date()));
        log.info("commit {}",df.format(new Date()));
        // 以上代码都是李老师模拟--在数据库中对 A账户减100块。
        // 如果上述代码发生异常或者执行不成功，认定失败了。
        // return LocalTransactionState.ROLLBACK_MESSAGE;
        // 本地事务执行成功返回
        // return LocalTransactionState.COMMIT_MESSAGE;
        // 这里是本地事务需要等待其他的(等待人脸识别的结果)
        return LocalTransactionState.UNKNOW;
    }

    /**
     * When no response to prepare(half) message. broker will send check message to check the transaction status, and this
     * method will be invoked to get local transaction status.
     * 当没有回应时，准备(半)消息。Broker将发送check消息来检查事务状态，并以此
     * 方法将被调用以获取本地事务状态。
     * 事务回查 默认是60s(不同的版本时间不同),一分钟检查一次
     *
     * @param msg Check message
     * @return Transaction state
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        // 设置日期格式
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        log.info("A服务器的本地事务回查, TransactionId: {}",msg.getTransactionId());
        log.info("commit {}",df.format(new Date()));
        // 这里检测到本地事务已经成功提交(检测 update A ...(A账户减100块) 这段SQL有没有执行成功)
        // todo 情况3.1: 业务回查成功!
        // return LocalTransactionState.COMMIT_MESSAGE;
        log.info("业务回查失败: 执行本地事务成功,确认消息");
        // 这里是本地事务需要等待其他的(等待人脸识别的结果)
        return LocalTransactionState.UNKNOW;
    }
}
