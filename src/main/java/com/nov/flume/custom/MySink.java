package com.nov.flume.custom;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 从配置文件中读取一个后缀，将event的内容读取后，拼接后缀进行输出
 * @author november
 */
public class MySink extends AbstractSink implements Configurable {
    private String keyName;

    private Logger logger= LoggerFactory.getLogger(MySink.class);

    /**
     * 核心方法
     * 处理sink逻辑
     * READY：成功传输了一个或多个event
     * BACKOFF：从channel中无法获取数据
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status status=Status.READY;

        //获取当前sink对接的channel
        Channel channel=getChannel();

        //声明一个event，用于接收chanel中的event
        Event event=null;

        //开启take事务
        Transaction transaction = channel.getTransaction();

        try {
            //开启事务
            transaction.begin();
            //如果channel中，没有可用的event,此时的event会是null
            if ((event = channel.take()) == null) {
                status = Status.BACKOFF;
            } else {
                //取到event数据后，执行拼接并输出
                logger.info(new String(event.getBody()) + keyName);
            }
            //提交事务
            transaction.commit();
        }catch (Exception e){
            e.printStackTrace();
            status=Status.BACKOFF;
            //回滚事务
            transaction.rollback();
        }finally {
            //关闭事务对象
            transaction.close();
        }

        return status;
    }

    /**
     * 从配置文件中读取信息
     * @param context
     */
    @Override
    public void configure(Context context) {
        keyName=context.getString("keyName","默认值");
    }
}
