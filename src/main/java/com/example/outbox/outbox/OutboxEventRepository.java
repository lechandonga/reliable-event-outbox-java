package com.example.outbox.outbox;

import com.example.outbox.config.OutboxProperties;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Propagation;

@Repository
public class OutboxEventRepository {

    private static final EventRowMapper ROW_MAPPER = new EventRowMapper();

    private final JdbcTemplate jdbcTemplate;

    public OutboxEventRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public long nextSequence(String aggregateKey) {
        jdbcTemplate.update("""
                INSERT INTO aggregate_sequence (aggregate_key, last_sequence_no)
                SELECT ?, 0 WHERE NOT EXISTS (SELECT 1 FROM aggregate_sequence WHERE aggregate_key = ?)
                """, aggregateKey, aggregateKey);
        int updated = jdbcTemplate.update("""
                UPDATE aggregate_sequence
                SET last_sequence_no = last_sequence_no + 1
                WHERE aggregate_key = ?
                """, aggregateKey);
        if (updated != 1) {
            throw new IllegalStateException("无法为聚合 " + aggregateKey + " 分配事件序号");
        }
        Long sequence = jdbcTemplate.queryForObject(
                "SELECT last_sequence_no FROM aggregate_sequence WHERE aggregate_key = ?",
                Long.class, aggregateKey);
        return sequence == null ? 1 : sequence;
    }

    @Transactional
    public void insert(OutboxEvent event) {
        jdbcTemplate.update("""
                INSERT INTO outbox_events (
                    id, aggregate_key, sequence_no, event_type, payload, status, attempts,
                    max_attempts, next_attempt_at, created_at, updated_at, version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, event.id(), event.aggregateKey(), event.sequenceNo(), event.eventType(), event.payload(),
                event.status().name(), event.attempts(), event.maxAttempts(), event.nextAttemptAt(),
                event.createdAt(), event.updatedAt(), event.version());
        insertHistory(event, null, event.status(), "业务事务原子写入事件");
    }

    @Transactional
    public List<OutboxEvent> claimDue(int batchSize, String workerId, Instant staleBefore, Instant now) {
        List<OutboxEvent> due = jdbcTemplate.query("""
                SELECT * FROM outbox_events
                WHERE status IN ('PENDING', 'IN_PROGRESS')
                  AND next_attempt_at <= ?
                  AND (locked_at IS NULL OR locked_at < ?)
                ORDER BY aggregate_key, sequence_no
                FETCH FIRST ? ROWS ONLY
                """, ROW_MAPPER, now, staleBefore, batchSize);
        java.util.ArrayList<OutboxEvent> claimed = new java.util.ArrayList<>();
        for (OutboxEvent event : due) {
            int attempts = event.attempts() + 1;
            int updated = jdbcTemplate.update("""
                    UPDATE outbox_events
                    SET status = 'IN_PROGRESS', attempts = ?, locked_at = ?, locked_by = ?,
                        last_error = NULL, updated_at = ?, version = version + 1
                    WHERE id = ? AND version = ?
                    """, attempts, now, workerId, now, event.id(), event.version());
            if (updated == 1) {
                insertHistory(event, event.status(), EventStatus.IN_PROGRESS, "投递器领取事件，第 " + attempts + " 次尝试");
                claimed.add(new OutboxEvent(event.id(), event.aggregateKey(), event.sequenceNo(),
                        event.eventType(), event.payload(), EventStatus.IN_PROGRESS, attempts,
                        event.maxAttempts(), event.nextAttemptAt(), now, workerId, event.sentAt(),
                        event.succeededAt(), event.deadAt(), null, event.createdAt(), now,
                        event.version() + 1));
            }
        }
        return claimed;
    }

    @Transactional
    public void markPublished(String eventId, Instant now) {
        jdbcTemplate.update("""
                UPDATE outbox_events SET sent_at = ?, updated_at = ?
                WHERE id = ? AND status = 'IN_PROGRESS'
                """, now, now, eventId);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean scheduleFailure(String eventId, String error, OutboxProperties properties, Instant now) {
        OutboxEvent event = findById(eventId).orElseThrow();
        if (event.status() == EventStatus.SUCCEEDED || event.status() == EventStatus.DEAD_LETTER) {
            return false;
        }
        int attempts = Math.max(event.attempts(), 1);
        if (attempts >= event.maxAttempts()) {
            return moveToDeadLetter(event, error, now);
        }
        Instant nextAttempt = now.plusMillis(properties.backoffMillis(attempts));
        int updated = jdbcTemplate.update("""
                UPDATE outbox_events
                SET status = 'PENDING', last_error = ?, next_attempt_at = ?,
                    locked_at = NULL, locked_by = NULL, updated_at = ?, version = version + 1
                WHERE id = ? AND status = 'IN_PROGRESS'
                """, error, nextAttempt, now, eventId);
        if (updated == 1) {
            insertHistory(event, EventStatus.IN_PROGRESS, EventStatus.PENDING,
                    "消费失败，安排退避重试：" + error);
        }
        return updated == 1;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean moveToDeadLetter(OutboxEvent event, String error, Instant now) {
        int updated = jdbcTemplate.update("""
                UPDATE outbox_events
                SET status = 'DEAD_LETTER', last_error = ?, dead_at = ?,
                    next_attempt_at = ?, locked_at = NULL, locked_by = NULL,
                    updated_at = ?, version = version + 1
                WHERE id = ? AND status <> 'SUCCEEDED'
                """, error, now, now, now, event.id());
        if (updated == 1) {
            insertHistory(event, event.status(), EventStatus.DEAD_LETTER,
                    "达到最大尝试次数，进入死信：" + error);
        }
        return updated == 1;
    }

    @Transactional
    public boolean markSucceeded(String eventId, Instant now) {
        OutboxEvent event = findById(eventId).orElseThrow();
        if (event.status() == EventStatus.SUCCEEDED) {
            return false;
        }
        int updated = jdbcTemplate.update("""
                UPDATE outbox_events
                SET status = 'SUCCEEDED', succeeded_at = COALESCE(succeeded_at, ?),
                    next_attempt_at = ?, locked_at = NULL, locked_by = NULL,
                    last_error = NULL, updated_at = ?, version = version + 1
                WHERE id = ? AND status <> 'SUCCEEDED'
                """, now, now, now, eventId);
        if (updated == 1) {
            insertHistory(event, event.status(), EventStatus.SUCCEEDED, "消费端事务提交成功");
        }
        return updated == 1;
    }

    @Transactional
    public boolean replay(String eventId, Instant now) {
        OutboxEvent event = findById(eventId).orElseThrow();
        if (event.status() == EventStatus.SUCCEEDED) {
            return false;
        }
        int updated = jdbcTemplate.update("""
                UPDATE outbox_events
                SET status = 'PENDING', attempts = 0, dead_at = NULL, last_error = NULL,
                    next_attempt_at = ?, locked_at = NULL, locked_by = NULL,
                    updated_at = ?, version = version + 1
                WHERE id = ? AND status = 'DEAD_LETTER'
                """, now, now, eventId);
        if (updated == 1) {
            insertHistory(event, EventStatus.DEAD_LETTER, EventStatus.PENDING, "人工重放死信");
        }
        return updated == 1;
    }

    public Optional<OutboxEvent> findById(String eventId) {
        return jdbcTemplate.query("SELECT * FROM outbox_events WHERE id = ?", ROW_MAPPER, eventId)
                .stream().findFirst();
    }

    public List<OutboxEvent> findAll() {
        return jdbcTemplate.query(
                "SELECT * FROM outbox_events ORDER BY aggregate_key, sequence_no", ROW_MAPPER);
    }

    public List<StatusHistory> findHistory(String eventId) {
        return jdbcTemplate.query("""
                SELECT * FROM event_status_history WHERE event_id = ? ORDER BY id
                """, (rs, rowNum) -> new StatusHistory(
                rs.getLong("id"), rs.getString("event_id"), rs.getString("aggregate_key"),
                rs.getLong("sequence_no"), parseStatus(rs.getString("from_status")),
                EventStatus.valueOf(rs.getString("to_status")), rs.getInt("attempts"),
                rs.getString("reason"), rs.getTimestamp("created_at").toInstant()), eventId);
    }

    @Transactional
    public boolean repairPayload(String eventId, String payload, Instant now) {
        int updated = jdbcTemplate.update("""
                UPDATE outbox_events
                SET payload = ?, updated_at = ?, version = version + 1
                WHERE id = ? AND status = 'DEAD_LETTER'
                """, payload, now, eventId);
        if (updated == 1) {
            OutboxEvent event = findById(eventId).orElseThrow();
            insertHistory(event, EventStatus.DEAD_LETTER, EventStatus.DEAD_LETTER, "人工修复事件载荷，等待重放");
        }
        return updated == 1;
    }

    private void insertHistory(OutboxEvent event, EventStatus from, EventStatus to, String reason) {
        Instant now = Instant.now();
        jdbcTemplate.update("""
                INSERT INTO event_status_history (
                    event_id, aggregate_key, sequence_no, from_status, to_status,
                    attempts, reason, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, event.id(), event.aggregateKey(), event.sequenceNo(),
                from == null ? null : from.name(), to.name(), event.attempts(), reason, now);
    }

    private static EventStatus parseStatus(String value) {
        return value == null ? null : EventStatus.valueOf(value);
    }

    private static class EventRowMapper implements RowMapper<OutboxEvent> {
        @Override
        public OutboxEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new OutboxEvent(
                    rs.getString("id"), rs.getString("aggregate_key"), rs.getLong("sequence_no"),
                    rs.getString("event_type"), rs.getString("payload"),
                    EventStatus.valueOf(rs.getString("status")), rs.getInt("attempts"),
                    rs.getInt("max_attempts"), instant(rs, "next_attempt_at"),
                    instant(rs, "locked_at"), rs.getString("locked_by"), instant(rs, "sent_at"),
                    instant(rs, "succeeded_at"), instant(rs, "dead_at"), rs.getString("last_error"),
                    instant(rs, "created_at"), instant(rs, "updated_at"), rs.getLong("version"));
        }

        private Instant instant(ResultSet rs, String column) throws SQLException {
            Timestamp timestamp = rs.getTimestamp(column);
            return timestamp == null ? null : timestamp.toInstant();
        }
    }
}
