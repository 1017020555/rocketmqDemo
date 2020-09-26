package com.sodo.demo.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import java.util.List;

/*
    消费者--负载均衡模式
 */
public class BalanceConsumer {

    public static void main(String[] args) throws Exception{

        DefaultMQPushConsumer consumer=new DefaultMQPushConsumer("group1");

        consumer.setNamesrvAddr("localhost:9876");

//   订阅topic
        consumer.subscribe("topic1","*");

//  负载均衡模式
        consumer.setMessageModel(MessageModel.CLUSTERING);

//  注册回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(
                    List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.printf("%s Receive New Messages: %s %n",
                        Thread.currentThread().getName(), list);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

//启动消费者
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
