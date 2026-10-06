package com.github.lechandonga.reliableeventoutboxjava;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.github.lechandonga.reliableeventoutboxjava.outbox.OutboxProperties;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(OutboxProperties.class)
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
