package com.sodo.demo.teansactionMessage;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @Author: lc
 * @ClassName: Consumer
 * @Pacakge: com.sodo.demo.teansactionMessage
 * @Date: 2020-09-26 21:51:48
 * @Description: TODO
 */
public class TransactionConsumer {

    //nameserver地址
    private static String namesrvaddress="127.0.0.1:9876;";


    public static void main(String[] args) throws MQClientException {
        //创建DefaultMQPushConsumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("transaction_consumer_group_name");
        //设置nameserver地址
        consumer.setNamesrvAddr(namesrvaddress);
        //设置每次拉去的消息个数
        consumer.setConsumeMessageBatchMaxSize(5);
        //设置消费顺序
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //设置监听的消息
        consumer.subscribe("TopicTxt_Demo","TagTx");
        //消息监听
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                try {
                    for (MessageExt msg : msgs) {
                        String topic = msg.getTopic();
                        String tags = msg.getTags();
                        String keys = msg.getKeys();
                        String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        System.out.println("topic:"+topic+",tags:"+tags+",keys:"+keys+",body:"+body);
                    }
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动消费
        consumer.start();
    }
}
