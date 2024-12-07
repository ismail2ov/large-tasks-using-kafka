package com.github.ismail2ov.largetasks;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
        List<String> tasks = List.of("1", "2", "3", "4", "5");
        List<String> expectedMessages = new ArrayList<>();

        for (String t : tasks) {
            kafkaTemplate.send(TaskConsumer.INPUT_TOPIC, t);
            expectedMessages.add(String.format("The status of task %s is %s", t, "FINISHED"));
        }

        Consumer<String, String> testConsumer = consumerFactory.createConsumer("test-consumer-group-id", "test");
        testConsumer.subscribe(List.of(TaskConsumer.STATUS_TOPIC));
        ConsumerRecords<String, String> received = KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(60L), 5);

        assertThat(received).extracting("value").containsExactlyElementsOf(expectedMessages);
    }
}