package com.example.Flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * 自定义拦截器
 */
public class MyInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    // 单个时间拦截
    @Override
    public Event intercept(Event event) {

        // 获取body信息
        String body = new String(event.getBody());

        // 判断并标记
        if (body.contains("hello")){
            event.getHeaders().put("type", "AA");
        }else {
            event.getHeaders().put("type", "BB");
        }

        return event;
    }

    // 批量事件拦截
    @Override
    public List<Event> intercept(List<Event> list) {

        for (Event event : list) {
            intercept(event);
        }

        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
