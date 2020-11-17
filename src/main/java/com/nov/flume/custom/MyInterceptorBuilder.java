package com.nov.flume.custom;

import org.apache.flume.Context;
import org.apache.flume.interceptor.Interceptor;

/**
 * flume拦截器需要额外提供一个内部builder,因为flume在创建拦截器对象时，固定调用builder来获取
 * @author november
 */
public class MyInterceptorBuilder implements Interceptor.Builder{

    /**
     * 返回一个当前拦截器对象
     * @return
     */
    @Override
    public Interceptor build() {
        return new MyInterceptor();
    }

    /**
     * 读取配置文件中的参数
     * @param context
     */
    @Override
    public void configure(Context context) {

    }
}
