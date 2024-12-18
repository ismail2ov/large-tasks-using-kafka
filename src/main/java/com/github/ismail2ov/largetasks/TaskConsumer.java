package com.github.ismail2ov.largetasks;

import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TaskConsumer {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String STATUS_TOPIC = "status-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public TaskConsumer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = INPUT_TOPIC)
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        try {
            log.info("Processing task {}", consumerRecord.value());

            Thread.sleep(Duration.ofSeconds(5));

            this.publishStatusOf(consumerRecord.value(), "FINISHED");
        } catch (InterruptedException e) {
            this.publishStatusOf(consumerRecord.value(), "ERROR");
        }
    }

    private void publishStatusOf(String value, String status) {
        String statusString = String.format("The status of task %s is %s", value, status);
        kafkaTemplate.send(STATUS_TOPIC, statusString);
    }

}
