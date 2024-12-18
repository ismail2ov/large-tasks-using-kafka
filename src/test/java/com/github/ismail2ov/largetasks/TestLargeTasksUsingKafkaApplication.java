package com.github.ismail2ov.largetasks;

import org.springframework.boot.SpringApplication;

public class TestLargeTasksUsingKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.from(LargeTasksUsingKafkaApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
