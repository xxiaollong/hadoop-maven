package com.example.flume;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.concurrent.TimeUnit;

/**
 * 自定义Source
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {

    String prefix;
    String subfix;

    @Override
    public void configure(Context context) {
        prefix = context.getString("prefix");
        subfix = context.getString("subfix", ".log");
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        try {
            for (int i = 0; i < 5; i++) {
                // 构建事件
                SimpleEvent event = new SimpleEvent();

                // 设置消息体
                event.setBody((prefix +"-"+i+"-"+subfix).getBytes());

                // 将事件传递给chanel
                getChannelProcessor().processEvent(event);

                // 修改状态
                status = Status.READY;
            }
        } catch (Exception e) {
            status = Status.BACKOFF;
            e.printStackTrace();
        }

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

}
