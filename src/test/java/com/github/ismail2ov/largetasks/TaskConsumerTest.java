package com.github.ismail2ov.largetasks;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class TaskConsumerTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @Test
    void testLargeTaskProcessing() {
        String task = "1";
        String expectedMessage = String.format("The status of task %s is %s", task, "FINISHED");

        kafkaTemplate.send(TaskConsumer.INPUT_TOPIC, task);

        Consumer<String, String> testConsumer = consumerFactory.createConsumer();
        testConsumer.subscribe(List.of(TaskConsumer.STATUS_TOPIC));
        ConsumerRecord<String, String> received = KafkaTestUtils.getSingleRecord(testConsumer, TaskConsumer.STATUS_TOPIC, Duration.ofSeconds(30L));

        assertThat(received.value()).isEqualTo(expectedMessage);
    }
}