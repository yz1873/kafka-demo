package com.example.demo;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
@Component
@Scope("prototype")
public class SimpleKafkaConsumer {
	private KafkaConsumer<String, String> consumer;

	public KafkaConsumer<String, String> getConsumer() {
		return consumer;
	}
	public SimpleKafkaConsumer() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.244.6.37:31090");
        //每个消费者分配独立的组号
        props.put("group.id", "test");

        //如果value合法，则自动提交偏移量
        props.put("enable.auto.commit", "true");
        //拉取消息位移，从最早的开始
        props.put("auto.offset.reset", "earliest");

        //设置多久一次更新被消费消息的偏移量
        props.put("auto.commit.interval.ms", "1000");

        //设置会话响应的时间，超过这个时间kafka可以选择放弃消费或者消费下一条消息
        props.put("session.timeout.ms", "30000");
        
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("ces11"));
    }
}