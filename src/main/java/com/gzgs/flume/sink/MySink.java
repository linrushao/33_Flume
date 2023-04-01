package com.gzgs.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 需求：将event通过logger打印到控制台
 *
 */
public class MySink extends AbstractSink implements Configurable {

    Logger logger = LoggerFactory.getLogger(MySink.class);

    /**
     * 核心处理方法
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {

        Status status;

        //1.获取channel
        Channel channel = getChannel();
        //2.获取一个事务
        Transaction tx = channel.getTransaction();
        //3.开启事务
        tx.begin();

        try{
            Event event;
            //4.take数据（保证能take到数据）
            while(true){
                event = channel.take();
                if(event == null){
                    TimeUnit.SECONDS.sleep(1);
                    continue;
                }
                break;
            }

            //5.处理event
            storSomeData(event);
            //6.提交事务
            tx.commit();
            status = Status.READY;

        }catch (Throwable t) {
            tx.rollback();
            status = Status.BACKOFF;
        }finally {
            tx.close();
        }
        return status;
    }

    private void storSomeData(Event event) {

        //对event的处理（写到本地文件，写到hdfs，写到MySQL....）
        //通过Log4j的技术将event打印到控制台
        logger.info(new String(event.getBody()));

    }

    @Override
    public void configure(Context context) {

    }
}
