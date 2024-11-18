package com.github.ismail2ov.largetasks;

import java.time.Duration;
import org.springframework.stereotype.Service;

@Service
public class LargeTaskProcessor {

    public boolean run(String task) throws InterruptedException {
        int seconds = (task.equals("2")) ? 10 : 5;
        Thread.sleep(Duration.ofSeconds(seconds));

        return true;
    }
}
