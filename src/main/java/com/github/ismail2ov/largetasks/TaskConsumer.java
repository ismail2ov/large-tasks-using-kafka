package com.github.ismail2ov.largetasks;

import java.util.Objects;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.support.TaskExecutorAdapter;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TaskConsumer {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String STATUS_TOPIC = "status-topic";
    public static final String CONTAINER_ID = "pausable-consumer";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final LargeTaskProcessor taskProcessor;
    private final KafkaContainerService kafkaContainerService;
    private final AsyncTaskExecutor executor;

    public TaskConsumer(KafkaTemplate<String, String> kafkaTemplate, LargeTaskProcessor taskProcessor, KafkaContainerService kafkaContainerService) {
        this.kafkaTemplate = kafkaTemplate;
        this.taskProcessor = taskProcessor;
        this.kafkaContainerService = kafkaContainerService;
        this.executor = new TaskExecutorAdapter(Executors.newSingleThreadExecutor());
    }

    @KafkaListener(id = CONTAINER_ID, topics = INPUT_TOPIC, idIsGroup = false)
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        kafkaContainerService.pauseConsume(CONTAINER_ID);

        executor.submitCompletable(() -> taskProcessor.run(consumerRecord.value()))
            .whenComplete((isSuccess, exception) -> {
                if (Objects.isNull(exception)) {
                    if (Boolean.TRUE.equals(isSuccess)) {
                        this.publishStatusOf(consumerRecord.value(), "FINISHED");
                    } else {
                        this.publishStatusOf(consumerRecord.value(), "FAILED");
                    }
                } else {
                    this.publishStatusOf(consumerRecord.value(), "ERROR");
                }
                kafkaContainerService.resumeConsumer(CONTAINER_ID);
            });
    }

    private void publishStatusOf(String value, String status) {
        String statusString = String.format("The status of task %s is %s", value, status);
        kafkaTemplate.send(STATUS_TOPIC, statusString);
    }

}
