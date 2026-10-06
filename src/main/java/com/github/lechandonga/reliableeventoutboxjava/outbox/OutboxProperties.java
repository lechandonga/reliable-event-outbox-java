package com.github.lechandonga.reliableeventoutboxjava.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "outbox")
public class OutboxProperties {

    /** 派发轮询间隔（毫秒） */
    private long pollIntervalMs = 500;
    /** 单次轮询最多捞取的事件数 */
    private int batchSize = 50;
    /** 消费工作线程数 */
    private int consumerThreads = 4;
    /** 最大尝试次数（含首次），超过后进入死信 */
    private int maxAttempts = 5;
    /** 首次重试退避（毫秒） */
    private long backoffInitialMs = 1000;
    /** 退避倍数 */
    private double backoffMultiplier = 2.0;
    /** 退避上限（毫秒） */
    private long backoffMaxMs = 30000;

    public long backoffForAttempt(int attempt) {
        double delay = backoffInitialMs * Math.pow(backoffMultiplier, Math.max(0, attempt - 1));
        return Math.min((long) delay, backoffMaxMs);
    }

    public long getPollIntervalMs() { return pollIntervalMs; }
    public void setPollIntervalMs(long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
    public int getConsumerThreads() { return consumerThreads; }
    public void setConsumerThreads(int consumerThreads) { this.consumerThreads = consumerThreads; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getBackoffInitialMs() { return backoffInitialMs; }
    public void setBackoffInitialMs(long backoffInitialMs) { this.backoffInitialMs = backoffInitialMs; }
    public double getBackoffMultiplier() { return backoffMultiplier; }
    public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
    public long getBackoffMaxMs() { return backoffMaxMs; }
    public void setBackoffMaxMs(long backoffMaxMs) { this.backoffMaxMs = backoffMaxMs; }
}
