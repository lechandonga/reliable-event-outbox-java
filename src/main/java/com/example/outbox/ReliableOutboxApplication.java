package com.example.outbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.example.outbox.config.OutboxProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
@EnableConfigurationProperties(OutboxProperties.class)
public class ReliableOutboxApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReliableOutboxApplication.class, args);
    }
}
