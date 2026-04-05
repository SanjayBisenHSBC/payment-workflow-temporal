package com.payment.workflow.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payment.workflow.kafka.model.FraudCheckResponse;
import com.payment.workflow.persistence.repository.FraudCheckRequestRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * FraudResponseConsumer — bridges the async Kafka fraud response back into
 * the synchronous Temporal activity execution.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * THE ASYNC-TO-SYNC BRIDGE PATTERN
 * ─────────────────────────────────────────────────────────────────────────────
 *
 * Temporal activities are synchronous (they block until they return a value).
 * Kafka is asynchronous (fire-and-forget publish, consume later).
 *
 * To connect them we use a ConcurrentHashMap of CompletableFutures:
 *
 *   FraudCheckActivityImpl:
 *     1. Creates a CompletableFuture and puts it in the pendingResponses map
 *        keyed by correlationId.
 *     2. Publishes the fraud request to Kafka.
 *     3. Calls future.get(timeout) — BLOCKS the activity thread.
 *
 *   FraudResponseConsumer (this class):
 *     4. Receives the response from the fraud system's Kafka topic.
 *     5. Looks up the CompletableFuture by correlationId.
 *     6. Calls future.complete(response) — UNBLOCKS the activity thread.
 *
 *   Back in FraudCheckActivityImpl:
 *     7. future.get() returns the FraudCheckResponse.
 *     8. Activity processes the result and returns the updated context.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * TIMEOUT HANDLING
 * ─────────────────────────────────────────────────────────────────────────────
 * If the fraud system doesn't respond within the configured timeout,
 * future.get() throws a TimeoutException. The activity can then:
 *   a) Fall back to internal rule-based scoring (fail-open strategy)
 *   b) Throw an exception so Temporal retries the whole activity (fail-safe)
 * This implementation uses strategy (a) — configurable in application.yml.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * MANUAL ACKNOWLEDGMENT
 * ─────────────────────────────────────────────────────────────────────────────
 * enable-auto-commit=false means we manually ack AFTER persisting the response
 * to the DB. This prevents message loss if the service crashes between receive
 * and DB write.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FraudResponseConsumer {

    private final FraudCheckRequestRepository fraudCheckRequestRepository;
    private final ObjectMapper objectMapper;

    /**
     * In-memory correlation map: correlationId → CompletableFuture.
     * Thread-safe. FraudCheckActivityImpl registers futures here before
     * publishing to Kafka; this consumer resolves them.
     */
    private final ConcurrentHashMap<String, CompletableFuture<FraudCheckResponse>>
        pendingResponses = new ConcurrentHashMap<>();

    // ── Consumer ─────────────────────────────────────────────────────────────

    @KafkaListener(
        topics = "${app.kafka.topics.fraud-response}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void onFraudResponse(
        @Payload FraudCheckResponse response,
        @Header(KafkaHeaders.RECEIVED_KEY) String key,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset,
        Acknowledgment ack
    ) {
        log.info("[KAFKA-CONSUMER] Received fraud response. correlationId={} riskLevel={} blocked={}",
            response.getCorrelationId(), response.getRiskLevel(), response.isBlocked());

        try {
            // ── 1. Persist response to DB ──────────────────────────────────
            fraudCheckRequestRepository.findByCorrelationId(response.getCorrelationId())
                .ifPresent(record -> {
                    try {
                        record.setResponsePayload(objectMapper.writeValueAsString(response));
                        record.setResponseReceivedAt(OffsetDateTime.now());
                        record.setStatus("RESPONDED");
                        record.setRiskScore(response.getRiskLevel());
                        record.setRiskDetails(response.getRiskDetails());
                        fraudCheckRequestRepository.save(record);
                        log.debug("[KAFKA-CONSUMER] Persisted fraud response for correlationId={}",
                            response.getCorrelationId());
                    } catch (Exception e) {
                        log.warn("[KAFKA-CONSUMER] Could not persist fraud response: {}", e.getMessage());
                    }
                });

            // ── 2. Complete the waiting CompletableFuture ──────────────────
            CompletableFuture<FraudCheckResponse> future =
                pendingResponses.remove(response.getCorrelationId());

            if (future != null) {
                future.complete(response);
                log.info("[KAFKA-CONSUMER] ✓ Unblocked waiting activity for correlationId={}",
                    response.getCorrelationId());
            } else {
                // This can happen if:
                // a) The service restarted after the request was sent (future was lost in memory)
                // b) The response arrived after the activity already timed out
                log.warn("[KAFKA-CONSUMER] No pending future found for correlationId={} " +
                    "(possible restart or timeout already occurred)", response.getCorrelationId());
            }

            // ── 3. Manual ack — only after DB write and future resolution ──
            ack.acknowledge();

        } catch (Exception e) {
            log.error("[KAFKA-CONSUMER] ✗ Error processing fraud response for correlationId={}: {}",
                response.getCorrelationId(), e.getMessage(), e);
            // Don't ack — Kafka will redeliver after rebalance/restart
        }
    }

    // ── Public API used by FraudCheckActivityImpl ─────────────────────────────

    /**
     * Registers a CompletableFuture that will be resolved when the fraud
     * system's response arrives on the Kafka topic.
     *
     * Called by FraudCheckActivityImpl BEFORE publishing the request.
     *
     * @param correlationId The correlation key matching request to response
     * @return A future that resolves to the FraudCheckResponse
     */
    public CompletableFuture<FraudCheckResponse> registerPendingRequest(String correlationId) {
        CompletableFuture<FraudCheckResponse> future = new CompletableFuture<>();
        pendingResponses.put(correlationId, future);
        log.debug("[KAFKA-CONSUMER] Registered pending future for correlationId={}", correlationId);
        return future;
    }

    /**
     * Waits for the fraud response, with a timeout.
     *
     * @param correlationId  The request's correlation ID
     * @param timeoutSeconds How long to wait before giving up
     * @return The fraud response, or null on timeout
     */
    public FraudCheckResponse waitForResponse(String correlationId, long timeoutSeconds) {
        CompletableFuture<FraudCheckResponse> future = pendingResponses.get(correlationId);
        if (future == null) {
            log.warn("[KAFKA-CONSUMER] No future registered for correlationId={}", correlationId);
            return null;
        }
        try {
            FraudCheckResponse response = future.get(timeoutSeconds, TimeUnit.SECONDS);
            log.info("[KAFKA-CONSUMER] ✓ Got fraud response for correlationId={} in time", correlationId);
            return response;
        } catch (java.util.concurrent.TimeoutException e) {
            log.warn("[KAFKA-CONSUMER] ✗ Timeout waiting for fraud response. correlationId={} timeout={}s",
                correlationId, timeoutSeconds);
            pendingResponses.remove(correlationId);

            // Mark DB record as TIMEOUT
            fraudCheckRequestRepository.findByCorrelationId(correlationId).ifPresent(record -> {
                record.setStatus("TIMEOUT");
                fraudCheckRequestRepository.save(record);
            });
            return null;
        } catch (Exception e) {
            log.error("[KAFKA-CONSUMER] Error waiting for fraud response: {}", e.getMessage());
            pendingResponses.remove(correlationId);
            return null;
        }
    }
}
