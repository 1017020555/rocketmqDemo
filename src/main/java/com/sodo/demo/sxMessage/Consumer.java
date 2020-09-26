package com.sodo.demo.sxMessage;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @Author: lc
 * @ClassName: Consumer
 * @Pacakge: com.sodo.demo.sxMessage
 * @Date: 2020-09-26 21:35:54
 * @Description: TODO
 */
public class Consumer {
    //nameserver地址
    private static String namesrvaddress="127.0.0.1:9876;";

    public static void main(String[] args) throws MQClientException {
        //创建消息消费对象DefaultMQConsumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_consumer_group_name");
        //设置nameserver地址
        consumer.setNamesrvAddr(namesrvaddress);

        //设置消费顺序
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //设置消息拉取最大数
        consumer.setConsumeMessageBatchMaxSize(5);

        //设置消费主题
        consumer.subscribe("Topic_Order_Demo","TagOrder");

        //消息监听
        consumer.setMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
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
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        //启动Consumer
        consumer.start();
    }
}
