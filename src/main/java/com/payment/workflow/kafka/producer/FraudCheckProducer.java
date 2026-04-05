package com.payment.workflow.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payment.workflow.kafka.model.FraudCheckRequestMessage;
import com.payment.workflow.persistence.entity.FraudCheckRequest;
import com.payment.workflow.persistence.repository.FraudCheckRequestRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.concurrent.CompletableFuture;

/**
 * FraudCheckProducer — publishes fraud screening requests to Kafka.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * FLOW
 * ─────────────────────────────────────────────────────────────────────────────
 *  1. FraudCheckActivityImpl calls sendFraudRequest(message)
 *  2. This class persists a PENDING fraud_check_requests row (idempotent)
 *  3. Publishes the message to payment.fraud.request using correlationId as key
 *  4. Kafka guarantees delivery (acks=all, retries=3, idempotent producer)
 *  5. The external fraud system consumes the message, computes a score,
 *     and publishes a response to payment.fraud.response
 *  6. FraudResponseConsumer receives the response and completes the waiting future
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * IDEMPOTENCY
 * ─────────────────────────────────────────────────────────────────────────────
 * If Temporal retries the FraudCheck activity, this method is called again
 * with the same correlationId. The DB check prevents duplicate rows.
 * The Kafka producer is also configured with enable.idempotence=true so
 * duplicate network retries at the broker level are deduplicated.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FraudCheckProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final FraudCheckRequestRepository fraudCheckRequestRepository;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topics.fraud-request}")
    private String fraudRequestTopic;

    /**
     * Sends a fraud check request to Kafka and persists the request record.
     *
     * @param message The fraud check request payload
     * @return CompletableFuture that resolves when the message is acknowledged by Kafka
     */
    @Transactional
    public CompletableFuture<SendResult<String, Object>> sendFraudRequest(FraudCheckRequestMessage message) {
        log.info("[KAFKA-PRODUCER] Sending fraud check request. paymentId={} correlationId={}",
            message.getPaymentId(), message.getCorrelationId());

        // Persist the request record (idempotent — skip if already exists)
        if (!fraudCheckRequestRepository.findByCorrelationId(message.getCorrelationId()).isPresent()) {
            try {
                FraudCheckRequest requestRecord = FraudCheckRequest.builder()
                    .paymentId(message.getPaymentId())
                    .correlationId(message.getCorrelationId())
                    .requestPayload(objectMapper.writeValueAsString(message))
                    .status("PENDING")
                    .requestSentAt(OffsetDateTime.now())
                    .build();
                fraudCheckRequestRepository.save(requestRecord);
                log.debug("[KAFKA-PRODUCER] Persisted fraud request record for correlationId={}", message.getCorrelationId());
            } catch (Exception e) {
                log.warn("[KAFKA-PRODUCER] Could not persist fraud request record: {}", e.getMessage());
                // Don't block the send — DB write failure here is non-fatal
            }
        } else {
            log.debug("[KAFKA-PRODUCER] Fraud request for correlationId={} already persisted (idempotent retry)",
                message.getCorrelationId());
        }

        // Publish to Kafka — use correlationId as the message key.
        // This ensures request and response land on the same partition (if
        // the fraud system echoes the key), preserving ordering.
        CompletableFuture<SendResult<String, Object>> future =
            kafkaTemplate.send(fraudRequestTopic, message.getCorrelationId(), message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[KAFKA-PRODUCER] ✗ Failed to send fraud request for correlationId={}: {}",
                    message.getCorrelationId(), ex.getMessage());
            } else {
                log.info("[KAFKA-PRODUCER] ✓ Fraud request sent. topic={} partition={} offset={}",
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            }
        });

        return future;
    }
}
