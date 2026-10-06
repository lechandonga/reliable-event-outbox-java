package com.github.lechandonga.reliableeventoutboxjava.outbox;

public record OutboxEvent(
        String id,
        String aggregateKey,
        long seq,
        String eventType,
        String payload,
        OutboxStatus status,
        int attempts,
        long nextRetryAt,
        String lastError,
        long createdAt,
        long updatedAt) {

    public enum OutboxStatus {
        /** 已写入，等待派发 */
        NEW,
        /** 已被派发器认领，正在投递途中 */
        DISPATCHED,
        /** 消费成功，终态 */
        DONE,
        /** 投递失败，等待按退避时间重试 */
        RETRY,
        /** 超过最大尝试次数，进入死信，等待人工重放 */
        DEAD
    }
}
