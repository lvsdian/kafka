package cn.andios.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @description:
 * @author:LSD
 * @when:2021/02/28/17:19
 */
public class CounterInterceptor implements ProducerInterceptor<String, String>{

    int succeed = 0;
    int failed = 0;

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            failed ++;
        } else {
            succeed ++;
        }
    }

    @Override
    public void close() {
        System.out.println("成功：" + succeed);
        System.out.println("失败：" + failed);
    }


}
