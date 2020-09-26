package com.sodo.demo.producer;


import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import java.io.UnsupportedEncodingException;

/*
    生产者同步发送
 */
public class SyncProductor {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        // 实例化消息生产者Producer
        DefaultMQProducer producer=new DefaultMQProducer("group1");
        // 设置NameServer的地址
        producer.setNamesrvAddr("localhost:9876");
        //启动producer
        producer.start();

        for (int i=0;i<100;i++){
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message(
                    "topic1" ,
                    "tag1" ,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            // 发送消息到一个Broker
            SendResult sendResult = producer.send(msg);
            // 通过sendResult返回消息是否成功送达
            System.out.printf("%s%n", sendResult);
        }

        producer.shutdown();
    }
}
