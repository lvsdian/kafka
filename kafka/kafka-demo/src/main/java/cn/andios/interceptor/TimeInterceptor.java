package cn.andios.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @description:
 * @author:LSD
 * @when:2021/02/28/17:18
 */
public class TimeInterceptor implements ProducerInterceptor<String, String>{

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 拼接上当前时间戳
        return new ProducerRecord<>(record.topic(), System.currentTimeMillis() + "-" + record.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }


}
