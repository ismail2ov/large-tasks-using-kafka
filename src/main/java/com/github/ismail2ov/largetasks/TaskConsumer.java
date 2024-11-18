package com.github.ismail2ov.largetasks;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class TaskConsumer {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String STATUS_TOPIC = "status-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final LargeTaskProcessor taskProcessor;

    @KafkaListener(topics = INPUT_TOPIC)
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        try {
            boolean isSuccess = taskProcessor.run();
            if (isSuccess) {
                this.publishStatusOf(consumerRecord.value(), "FINISHED");
            } else {
                this.publishStatusOf(consumerRecord.value(), "FAILED");
            }
        } catch (Exception e) {
            this.publishStatusOf(consumerRecord.value(), "ERROR");
            throw new RuntimeException(e);
        }
    }

    private void publishStatusOf(String value, String status) {
        String statusString = String.format("The status of task %s is %s", value, status);
        kafkaTemplate.send(STATUS_TOPIC, statusString);
    }

}
