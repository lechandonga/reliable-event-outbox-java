package com.github.lechandonga.reliableeventoutboxjava.web;

import com.github.lechandonga.reliableeventoutboxjava.outbox.OutboxDispatcher;
import com.github.lechandonga.reliableeventoutboxjava.outbox.OutboxEvent;
import com.github.lechandonga.reliableeventoutboxjava.outbox.OutboxEventRepository;
import com.github.lechandonga.reliableeventoutboxjava.outbox.OutboxRecovery;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 手工触发与状态查询入口。
 */
@RestController
@RequestMapping("/outbox")
public class OutboxController {

    private final OutboxEventRepository repository;
    private final OutboxDispatcher dispatcher;
    private final OutboxRecovery recovery;

    public OutboxController(OutboxEventRepository repository,
                            OutboxDispatcher dispatcher,
                            OutboxRecovery recovery) {
        this.repository = repository;
        this.dispatcher = dispatcher;
        this.recovery = recovery;
    }

    @GetMapping("/events")
    public List<OutboxEvent> all() {
        return repository.findAll();
    }

    @GetMapping("/events/{id}")
    public ResponseEntity<OutboxEvent> byId(@PathVariable String id) {
        return repository.findById(id).map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/dead")
    public List<OutboxEvent> dead() {
        return repository.findByStatus(OutboxEvent.OutboxStatus.DEAD);
    }

    @GetMapping("/stats")
    public Map<String, Integer> stats() {
        Map<String, Integer> stats = new LinkedHashMap<>();
        for (OutboxEvent.OutboxStatus s : OutboxEvent.OutboxStatus.values()) {
            stats.put(s.name(), repository.findByStatus(s).size());
        }
        return stats;
    }

    /** 手工触发一次派发 */
    @PostMapping("/dispatch")
    public Map<String, Integer> dispatch() {
        return Map.of("dispatched", dispatcher.pollNow());
    }

    /** 人工重放死信事件；非 DEAD 状态返回 409，DONE 事件绝不可重放 */
    @PostMapping("/events/{id}/replay")
    public ResponseEntity<Map<String, Object>> replay(@PathVariable String id) {
        boolean ok = repository.replay(id, System.currentTimeMillis());
        if (!ok) {
            return ResponseEntity.status(409).body(Map.of(
                    "replayed", false,
                    "reason", "event not found or not in DEAD status"));
        }
        return ResponseEntity.ok(Map.of("replayed", true, "id", id));
    }

    /** 手工触发崩溃恢复（把卡住的 DISPATCHED 重置为 NEW） */
    @PostMapping("/recover")
    public Map<String, Integer> recover() {
        return Map.of("recovered", recovery.recoverStuckEvents());
    }
}
