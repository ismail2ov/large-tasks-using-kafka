package com.github.ismail2ov.largetasks;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaContainerService {

    private final KafkaListenerEndpointRegistry registry;


    public void pauseConsume(String containerId) {

        getContainer(containerId).ifPresent(MessageListenerContainer::pause);
    }


    public void resumeConsumer(String containerId) {

        getContainer(containerId).ifPresent(MessageListenerContainer::resume);
    }


    private Optional<MessageListenerContainer> getContainer(String containerId) {

        return Optional.ofNullable(registry.getListenerContainer(containerId));

    }
}
