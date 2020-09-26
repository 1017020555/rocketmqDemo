package com.sodo.demo.teansactionMessage;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

/**
 * @Author: lc
 * @ClassName: TransactionProducer
 * @Pacakge: com.sodo.demo.teansactionMessage
 * @Date: 2020-09-26 21:52:59
 * @Description: TODO
 */
public class TransactionProducer {

    //nameserver地址
    private static String namesrvaddress="127.0.0.1:9876;";

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, InterruptedException {
        //创建事务消息发送对象
        TransactionMQProducer producer = new TransactionMQProducer("transaction_producer_group_name");
        //设置namesrv地址
        producer.setNamesrvAddr(namesrvaddress);
        //创建监听器
        TransactionListener transactionListener = new TransactionListenerImpl();
        //创建线程池
        ExecutorService executorService = new ThreadPoolExecutor(
                2,
                5,
                100,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(
                        2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable runnable) {
                        Thread thread = new Thread(runnable);
                        thread.setName("client-transaction-msg-check-thread");
                        return thread;
                    }
                }
        );

        //设置线程池
        producer.setExecutorService(executorService);
        //设置监听器
        producer.setTransactionListener(transactionListener);
        //启动producer
        producer.start();

        //创建消息
        Message message = new Message(
                "TopicTxt_Demo",
                "TagTx",
                "KeyTx1",
                "hello".getBytes(RemotingHelper.DEFAULT_CHARSET));

        //发送事务消息,此时消息不可见
        TransactionSendResult transactionSendResult = producer.sendMessageInTransaction(message, "发送消息，回传所需数据！");
        System.out.println(transactionSendResult);


        //休眠
        Thread.sleep(120000);
        //关闭
        producer.shutdown();
    }
}
