package com.sodo.demo.teansactionMessage;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: lc
 * @ClassName: TransactionListenerImpl
 * @Pacakge: com.sodo.demo.teansactionMessage
 * @Date: 2020-09-26 21:52:02
 * @Description: TODO
 */
public class TransactionListenerImpl implements TransactionListener {

    //存储当前线程对应的事务状态
    private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();

    /***
     * 发送prepare消息成功后回调该方法用于执行本地事务
     * @param msg:回传的消息，利用transactionId即可获取到该消息的唯一Id
     * @param arg:调用send方法时传递的参数，当send时候若有额外的参数可以传递到send方法中，这里能获取到
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        //获取线程ID
        String transactionId = msg.getTransactionId();
        //初始状态为0
        localTrans.put(transactionId,0);

        try {
            //此处执行本地事务操作
            System.out.println("....执行本地事务");
            Thread.sleep(70000);
            System.out.println("....执行完成本地事务");
        } catch (InterruptedException e) {
            e.printStackTrace();
            //发生异常，则回滚消息
            localTrans.put(transactionId,2);
            return LocalTransactionState.UNKNOW;
        }

        //修改状态
        localTrans.put(transactionId,1);
        System.out.println("executeLocalTransaction------状态为1");
        //本地事务操作如果成功了，则提交该消息，让该消息可见
        return LocalTransactionState.UNKNOW;
    }

    /***
     * 消息回查
     * @param msg
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        //获取事务id
        String transactionId = msg.getTransactionId();

        //通过事务id获取对应的本地事务执行状态
        Integer status = localTrans.get(transactionId);
        System.out.println("消息回查-----"+status);
        switch (status){
            case 0:
                return LocalTransactionState.UNKNOW;
            case 1:
                return LocalTransactionState.COMMIT_MESSAGE;
            case 2:
                return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return LocalTransactionState.UNKNOW;
    }
}