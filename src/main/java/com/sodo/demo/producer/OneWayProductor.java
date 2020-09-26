package com.sodo.demo.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
/*
    生产者单向发送
 */
public class OneWayProductor {

    public static void main(String[] args) throws Exception{

        DefaultMQProducer producer=new DefaultMQProducer("group3");

        producer.setNamesrvAddr("localhost:9876");

        producer.start();

        for (int i=0;i<100;i++){

            Message message = new Message(
                    "topic3" ,
                    "tag3" ,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)//DEFAULT_CHARSET = "UTF-8"
            );

// 发送单向消息，没有任何返回结果
            producer.sendOneway(message);
        };

        producer.shutdown();
    }
}
