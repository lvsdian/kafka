package cn.andios.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @description: 自动提交offset
 * @author:LSD
 * @when:2021/02/28/15:52
 */
public class AutoCommitConsumer {
    public static void main(String[] args) {
        // 消费者配置信息
        Properties props = new Properties();

        // 连接的集群
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        // 消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 自动提交延时
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // KV反序列化
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅主题
        consumer.subscribe(Arrays.asList("first", "second"));
        while (true) {
            // 获取数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            // 打印
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("partition = %d，offset = %d, key = %s, value = %s%n",record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }
}

