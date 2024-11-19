package com.github.ismail2ov.largetasks;

import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class LargeTaskProcessor {

    public boolean run(String task) throws InterruptedException {
        log.info("Processing task {}", task);

        int seconds = (task.equals("2")) ? 10 : 5;
        Thread.sleep(Duration.ofSeconds(seconds));

        return true;
    }
}
