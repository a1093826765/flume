package com.nov.flume.custom;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * flume自定义拦截器
 * 为每个event的header中添加key-value:time=时间戳
 * @author november
 */
public class MyInterceptor implements Interceptor {

    /**
     * 初始化
     */
    @Override
    public void initialize() {

    }

    /**
     * 拦截处理方法
     * 处理单个event
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        Map<String,String> headers=event.getHeaders();
        headers.put("time",System.currentTimeMillis()+"");
        return null;
    }

    /**
     * 拦截处理方法
     * 处理eventList
     * @param list
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> list) {
        for(Event event:list){
            intercept(event);
        }
        return list;
    }

    /**
     * 结束时调用此方法
     */
    @Override
    public void close() {

    }


}
