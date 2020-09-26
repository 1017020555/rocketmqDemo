package com.sodo.demo.ptMessage;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @author  lc
 * @date  2020
 * @description 普通消息生产者
 */
public class Producer {
/*
消息发送步骤：
1.创建DefaultMQProducer
2.设置Namesrv地址
3.开启DefaultMQProducer
4.创建消息Message
5.发送消息
6.关闭DefaultMQProducer
 */
//指定namesrv地址
private static String NAMESRV_ADDRESS = "127.0.0.1:9876";

    public static void main(String[] args) throws Exception {
        //创建一个DefaultMQProducer,需要指定消息发送组
        DefaultMQProducer producer = new DefaultMQProducer("producer1");

        //指定Namesvr地址
        producer.setNamesrvAddr(NAMESRV_ADDRESS);

        //启动Producer
        producer.start();

        //创建消息
        Message message = new Message(
                "topic1",     //主题
                "tag1",                  //标签，可以用来做过滤
                "key1",                  //唯一标识，可以用来查找消息
                "hello rocketmq".getBytes()  //要发送的消息字节数组
        );

        //发送消息
        SendResult result = producer.send(message,2000);

        //关闭producer
        producer.shutdown();
    }
}
