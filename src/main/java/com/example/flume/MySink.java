package com.example.flume;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 自定义Sink
 */
public class MySink extends AbstractSink implements Configurable {
    Logger logger = LoggerFactory.getLogger(MySink.class);

    String prefix;
    String subfix;

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        // 获取通道
        Channel channel = getChannel();

        // 获取事务
        Transaction transaction = channel.getTransaction();

        // 开启事务
        transaction.begin();

        try {
            // 获取事件
            Event event = channel.take();

            if (event != null){
                // 执行业务操作
                String body = new String(event.getBody());
                logger.info(prefix +body+subfix);
            }

            // 提交事务
            transaction.commit();
            // 修改状态
            status = Status.READY;
        } catch (ChannelException e) {
            // 回滚事务
            transaction.rollback();
            status = Status.BACKOFF;
        }finally {
            transaction.close();
        }


        return status;
    }

    @Override
    public void configure(Context context) {
        prefix = context.getString("prefix");
        subfix = context.getString("subfix", "--end");
    }
}
