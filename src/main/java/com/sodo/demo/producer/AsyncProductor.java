package com.sodo.demo.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/*
  生产者异步发送
 */
public class AsyncProductor {

    public static void main(String[] args) throws Exception{
//        实例化Productor
        DefaultMQProducer producer=new DefaultMQProducer("group2");
//设置NameServer地址
        producer.setNamesrvAddr("localhost:9876");

        producer.start();

//异步发送失败重试次数
        producer.setRetryTimesWhenSendAsyncFailed(0);

        for (int i = 0; i < 100; i++) {

            final int index = i;

            Message message=new Message(
                    "topic2",
                    "tag2",
                    "key2",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));

//      SendCallback接收异步返回结果的回调
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%-10d OK %s %n", index,
                            sendResult.getMsgId());
                }
                @Override
                public void onException(Throwable e) {
                    System.out.printf("%-10d Exception %s %n", index, e);
                    e.printStackTrace();
                }
            });

        }

        producer.shutdown();
    }
}
