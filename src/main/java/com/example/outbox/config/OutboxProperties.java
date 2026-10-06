package com.example.outbox.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "outbox.relay")
public record OutboxProperties(
        long fixedDelayMs,
        int batchSize,
        int maxAttempts,
        long backoffInitialMs,
        double backoffMultiplier,
        long backoffMaxMs,
        int workerThreads,
        long inProgressTimeoutMs
) {
    public long backoffMillis(int attempts) {
        double delay = backoffInitialMs
                * Math.pow(Math.max(1, backoffMultiplier), Math.max(0, attempts - 1));
        return Math.min(backoffMaxMs, Math.max(0L, Math.round(delay)));
    }
}
