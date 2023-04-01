package com.gzgs.flume.source;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 需求：每一秒钟随机生成一个字符串（模拟日志）
 *
 * 自定义Source需要继承AbstractSource，并实现Configurable PollableSource接口
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {

    private String prefix;

    /**
     * Source采集数据的核心过程
     *
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Status status;

        try {

            //采集一条新的数据(从文件读取，从端口读取，从MySQL读取，从xxx读取)
            Event e = getSomeData();

            //将event传入到channel Processor进行处理(拦截器，channelSelector)
            getChannelProcessor().processEvent(e);

            //event处理成功
            status = Status.READY;

        }catch (Throwable t){
            status = Status.BACKOFF;
        }

        return status;
    }

    private Event getSomeData() {

        String log = UUID.randomUUID().toString();
        SimpleEvent event = new SimpleEvent();
        event.setBody((prefix + log).getBytes());


        return event;

    }


    /**
     * 每次back off时间的增长
     * @return
     */
    @Override
    public long getBackOffSleepIncrement() {
        return 1000;
    }

    /**
     * 最大的back off时间
     * @return
     */
    @Override
    public long getMaxBackOffSleepInterval() {
        return 10000;
    }

    /**
     * 读取配置文件中的内容
     * @param context
     */
    @Override
    public void configure(Context context) {

        prefix = context.getString("prefix","log-");

    }
}
