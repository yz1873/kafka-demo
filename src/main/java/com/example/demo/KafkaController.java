package com.example.demo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@Scope("prototype")
public class KafkaController {
    @Autowired
    private SimpleKafkaConsumer consumer;

    @Autowired
    private SimpleKafkaProducer producer;

    @RequestMapping("receive")
    @ResponseBody
    public String receive() {

        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(3);

        fixedThreadPool.execute(new Runnable() {

            @Override

            public void run() {
                KafkaConsumer<String, String> kafkaconsumer = consumer.getConsumer();
                try {
                    while (true) {
                        ConsumerRecords<String, String> records = kafkaconsumer.poll(100);
                        for (ConsumerRecord<String, String> record : records)

                            System.out.printf(records.count() + "!!!!!!!!offset = %d, key = %s, value = %s\n", record.offset(),
                                    record.key(), record.value());
                    }
                } finally {
                    kafkaconsumer.close();
                }


            }

        });
        return "success";
    }

    @RequestMapping("send")
    @ResponseBody
    public String send() {
        producer.prouduce();
        return "success";
    }


}
