package com.gzgs.flume.interceptor;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * 需求：将event按照是否包含“linrushao"字母，给每个event添加对应的header
 *
 * 自定义Interceptor需要实现Interceptor接口,并提供实现Builder接口的内部类，用于创建当前的拦截器对象
 *
 */
public class LogInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }


    /**
     * 对单个Event进行拦截处理
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {

        //1.获取event中的body数据
        String body = new String(event.getBody());
        //2.获取event中的headers
        Map<String, String> headers = event.getHeaders();
        //3.判断body中是否包含有”linrushao“
        if(body.contains("linrushao")){
            headers.put("title","at");
        }else {
            headers.put("title","ot");
        }

//        event.setHeaders(headers);

        return event;
    }

    /**
     * 对多个Event进行拦截处理
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {

        for(Event event : events){
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder{

        @Override
        public Interceptor build() {
            return new LogInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }


}
