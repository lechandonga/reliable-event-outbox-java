package com.github.lechandonga.reliableeventoutboxjava.outbox;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

@Repository
public class OutboxEventRepository {

    private final JdbcTemplate jdbc;

    public OutboxEventRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final RowMapper<OutboxEvent> MAPPER = new RowMapper<>() {
        @Override
        public OutboxEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new OutboxEvent(
                    rs.getString("id"),
                    rs.getString("aggregate_key"),
                    rs.getLong("seq"),
                    rs.getString("event_type"),
                    rs.getString("payload"),
                    OutboxEvent.OutboxStatus.valueOf(rs.getString("status")),
                    rs.getInt("attempts"),
                    rs.getLong("next_retry_at"),
                    rs.getString("last_error"),
                    rs.getLong("created_at"),
                    rs.getLong("updated_at"));
        }
    };

    /** 与业务写入处于同一事务时调用，保证原子提交 */
    public void insert(OutboxEvent e) {
        jdbc.update("""
                INSERT INTO outbox_event
                (id, aggregate_key, seq, event_type, payload, status, attempts, next_retry_at, last_error, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                e.id(), e.aggregateKey(), e.seq(), e.eventType(), e.payload(),
                e.status().name(), e.attempts(), e.nextRetryAt(), e.lastError(),
                e.createdAt(), e.updatedAt());
    }

    public long nextSeq(String aggregateKey) {
        Long max = jdbc.queryForObject(
                "SELECT MAX(seq) FROM outbox_event WHERE aggregate_key = ?", Long.class, aggregateKey);
        return (max == null ? 0 : max) + 1;
    }

    /**
     * 捞取到期可派发的事件：NEW 或 RETRY 且退避时间已到；
     * 同一聚合键只取最靠前的一条未完成事件，保证按键有序。
     */
    public List<OutboxEvent> findDispatchable(int limit, long now) {
        return jdbc.query("""
                SELECT * FROM outbox_event e
                WHERE e.status IN ('NEW','RETRY') AND e.next_retry_at <= ?
                  AND NOT EXISTS (
                      SELECT 1 FROM outbox_event p
                      WHERE p.aggregate_key = e.aggregate_key
                        AND p.seq < e.seq
                        AND p.status <> 'DONE')
                ORDER BY e.aggregate_key, e.seq
                LIMIT ?
                """, MAPPER, now, limit);
    }

    /** 乐观认领：只有仍处于 NEW/RETRY 的事件才能被认领为 DISPATCHED */
    public boolean claim(String id, long now) {
        return jdbc.update("""
                UPDATE outbox_event SET status='DISPATCHED', updated_at=?
                WHERE id=? AND status IN ('NEW','RETRY')
                """, now, id) == 1;
    }

    public void markDone(String id, long now) {
        jdbc.update("UPDATE outbox_event SET status='DONE', last_error=NULL, updated_at=? WHERE id=?",
                now, id);
    }

    public void markRetry(String id, int attempts, long nextRetryAt, String error, long now) {
        jdbc.update("""
                UPDATE outbox_event SET status='RETRY', attempts=?, next_retry_at=?, last_error=?, updated_at=?
                WHERE id=?
                """, attempts, nextRetryAt, truncate(error), now, id);
    }

    public void markDead(String id, int attempts, String error, long now) {
        jdbc.update("""
                UPDATE outbox_event SET status='DEAD', attempts=?, last_error=?, updated_at=?
                WHERE id=?
                """, attempts, truncate(error), now, id);
    }

    /** 人工重放：仅允许 DEAD -> NEW，且清零尝试次数；DONE 事件绝不回退 */
    public boolean replay(String id, long now) {
        return jdbc.update("""
                UPDATE outbox_event SET status='NEW', attempts=0, next_retry_at=0, last_error=NULL, updated_at=?
                WHERE id=? AND status='DEAD'
                """, now, id) == 1;
    }

    /** 崩溃恢复：进程退出时停留在 DISPATCHED 的事件重新置为 NEW */
    public int resetStuckDispatched(long now) {
        return jdbc.update("UPDATE outbox_event SET status='NEW', updated_at=? WHERE status='DISPATCHED'",
                now);
    }

    public Optional<OutboxEvent> findById(String id) {
        List<OutboxEvent> list = jdbc.query("SELECT * FROM outbox_event WHERE id=?", MAPPER, id);
        return list.stream().findFirst();
    }

    public List<OutboxEvent> findAll() {
        return jdbc.query("SELECT * FROM outbox_event ORDER BY aggregate_key, seq", MAPPER);
    }

    public List<OutboxEvent> findByStatus(OutboxEvent.OutboxStatus status) {
        return jdbc.query("SELECT * FROM outbox_event WHERE status=? ORDER BY aggregate_key, seq",
                MAPPER, status.name());
    }

    private static String truncate(String s) {
        if (s == null) return null;
        return s.length() > 1000 ? s.substring(0, 1000) : s;
    }
}
