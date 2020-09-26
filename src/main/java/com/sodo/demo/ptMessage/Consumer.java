package com.sodo.demo.ptMessage;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @Author: lc
 * @ClassName: Consumer
 * @Pacakge: com.sodo.demo.ptMessage
 * @Date: 2020-09-26 17:04:55
 * @Description: TODO
 */
public class Consumer {
/*
消费者消费消息有这么几个步骤：
1.创建DefaultMQPushConsumer
2.设置namesrv地址
3.设置subscribe，这里是要读取的主题信息
4.创建消息监听MessageListener
5.获取消息信息
6.返回消息读取状态
 */
    //指定namesrv地址
    private static String NAMESRV_ADDRESS = "127.0.0.1:9876";

    public static void main(String[] args) throws MQClientException {

        DefaultMQPushConsumer consumer=new DefaultMQPushConsumer("consumer1");

        consumer.setNamesrvAddr(NAMESRV_ADDRESS);

        consumer.subscribe(
                "topic1",     //指定要读取的消息主题
                "tag1");  //指定要读取的消息过滤信息,多个标签数据，则可以输入"tag1 || tag2 || tag3"

        consumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list,
                                                            ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                String result= null;
                try {
                    //获取第1个消息
                    MessageExt messageExt = list.get(0);
//                    获取主题
                    String topic = messageExt.getTopic();
//                    获取标签
                    String tags = messageExt.getTags();
//                    获取消息
                    result = new String(messageExt.getBody(),"UTF-8");

                    System.out.println("topic:"+topic+",tags:"+tags+",result:"+result);

                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
//                 消息重试
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
//              消息消费成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
//      启动消费
        consumer.start();
    }
}
