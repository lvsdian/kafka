package cn.andios.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @description: 异步
 * @author:LSD
 * @when:2021/02/28/14:51
 */
public class AsyncProducer {
    public static void main(String[] args) {
        // 创建kafka生产者的配置信息
        Properties props = new Properties();

        // kafka 集群，broker-list
        props.put("bootstrap.servers", "hadoop102:9092");
        // 应答级别
        props.put("acks", "all");
        // 重试次数
        props.put("retries", 3);
        // 批次大小
        props.put("batch.size", 16384);
        // 等待时间
        props.put("linger.ms", 1);
        // RecordAccumulator 缓冲区大小
        props.put("buffer.memory", 33554432);
        // KV序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 发送数据，不带回调
        for (int i = 0; i < 100; i++) {
            // 指定topic、value
            producer.send(new ProducerRecord<String, String>("first", "andios-" + i));
            // 指定topic、partition、key、value
            producer.send(new ProducerRecord<String, String>("first", 0, "andios", "andios-" + i));
        }

        // 发送数据，带回调
        for (int i = 0; i < 100; i++) {
            // 回调函数，该方法会在 Producer 收到 ack 时调用，为异步调用
            producer.send(new ProducerRecord<>("first", "andios-" + i), (recordMetadata, e) -> {
                // 消息发送失败会自动重试，不需要我们在回调函数中手动重试。
                // 如果 Exception 为 null，说明消息发送成功，如果Exception 不为 null，说明消息发送失败。
                if (e == null) {
                    System.out.println("success->offset:" + recordMetadata.offset() + ",partition:" + recordMetadata.partition());
                } else {
                    e.printStackTrace();
                }
            });

            producer.close();
        }
    }
}
