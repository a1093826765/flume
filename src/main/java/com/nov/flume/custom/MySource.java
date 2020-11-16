package com.nov.flume.custom;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.ArrayList;
import java.util.List;

/**
 * 使用flume接收数据，并给每条数据添加前缀，输出到控制台。前缀可以从flume配置文件获取
 * @author november
 */
public class MySource  extends AbstractSource implements Configurable, PollableSource {

    private String keyName;

    /**
     * 核心方法，在此方法中创建event，将event放入channel
     * Status={ READY , BACKOFF }
     * READY: source成功封装event，存入channel，返回READY
     * BACKOFF：source无法封装event，无法存入channel，返回BACKOFF
     * process()方法会被source所在的线程循环调用
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status status=Status.READY;

        //封装event
        List<Event> eventList=new ArrayList<Event>();
        for(int i=0;i<10;i++){
            SimpleEvent simpleEvent=new SimpleEvent();
            //向body中封装数据
            simpleEvent.setBody((keyName+"hello"+i).getBytes());
            eventList.add(simpleEvent);
        }

        //将数据放入channel
        try {
            //获取当前source对象对应的ChannelProcessor
            ChannelProcessor channelProcessor = getChannelProcessor();
            channelProcessor.processEventBatch(eventList);
        }catch (Exception e){
            e.printStackTrace();
            status=Status.BACKOFF;
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

    /**
     * 从配置文件中读取信息
     * @param context
     */
    @Override
    public void configure(Context context) {
        //从配置文件中读取key值，假如没有有该值，则使用默认值
        keyName=context.getString("keyName","默认值：");
    }
}
