package com.github.ismail2ov.largetasks;

import java.time.Duration;
import org.springframework.stereotype.Service;

@Service
public class LargeTaskProcessor {

    public boolean run(String task) throws InterruptedException {
        Thread.sleep(Duration.ofSeconds(5));

        return true;
    }
}
