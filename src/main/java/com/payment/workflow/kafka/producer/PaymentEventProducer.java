package com.payment.workflow.kafka.producer;

import com.payment.workflow.kafka.model.PaymentEventMessage;
import com.payment.workflow.model.PaymentContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * PaymentEventProducer — publishes payment lifecycle events to Kafka.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * PURPOSE
 * ─────────────────────────────────────────────────────────────────────────────
 * After each workflow exit point completes, the corresponding activity calls
 * publishEvent() to broadcast the lifecycle change on payment.events.
 *
 * Downstream consumers of payment.events include:
 *  - Notification Service  → sends email/SMS to customers
 *  - Reporting Pipeline    → feeds BI dashboards and reconciliation jobs
 *  - ERP / Core Banking    → triggers downstream booking entries
 *  - Audit Service         → archives immutable compliance records
 *  - Alerting              → monitors for failures and SLA breaches
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * KEY STRATEGY
 * ─────────────────────────────────────────────────────────────────────────────
 * paymentId is used as the Kafka message key. This guarantees all events for
 * a single payment are written to the same partition, preserving order.
 * Consumers that care about ordering (e.g., state machines) can rely on this.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topics.payment-events}")
    private String paymentEventsTopic;

    // ── Convenience methods per exit point ────────────────────────────────────

    public void publishInitiated(PaymentContext ctx) {
        publish(buildEvent(ctx, "PAYMENT_INITIATED",
            "Payment booked with ID: " + ctx.getAssignedPaymentId()));
    }

    public void publishValidated(PaymentContext ctx) {
        publish(buildEvent(ctx, "PAYMENT_VALIDATED",
            "Validation passed. Route: " + ctx.getPaymentRoute()
                + ", ValueDate: " + ctx.getValueDate()));
    }

    public void publishFraudCleared(PaymentContext ctx) {
        publish(buildEvent(ctx, "FRAUD_CLEARED",
            "Fraud check passed. Risk: " + ctx.getFraudRiskScore()));
    }

    public void publishFraudBlocked(PaymentContext ctx) {
        publish(buildEvent(ctx, "FRAUD_BLOCKED",
            "Payment blocked. " + ctx.getFraudCheckDetails()));
    }

    public void publishAccountingPosted(PaymentContext ctx) {
        publish(buildEvent(ctx, "ACCOUNTING_POSTED",
            "GL entries posted. Ref: " + ctx.getAccountingReference()));
    }

    public void publishCompleted(PaymentContext ctx) {
        publish(buildEvent(ctx, "PAYMENT_COMPLETED",
            "Payment completed. TXN: " + ctx.getTransactionReference()));
    }

    public void publishFailed(PaymentContext ctx, String reason) {
        publish(buildEvent(ctx, "PAYMENT_FAILED", "Payment failed: " + reason));
    }

    // ── Core publish method ───────────────────────────────────────────────────

    public void publish(PaymentEventMessage event) {
        try {
            var future = kafkaTemplate.send(paymentEventsTopic, event.getPaymentId(), event);
            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.warn("[KAFKA-EVENT] ✗ Failed to publish event {} for payment {}: {}",
                        event.getEventType(), event.getPaymentId(), ex.getMessage());
                } else {
                    log.debug("[KAFKA-EVENT] ✓ Event published: type={} paymentId={} partition={} offset={}",
                        event.getEventType(), event.getPaymentId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                }
            });
        } catch (Exception e) {
            // Event publish failures must NOT fail the payment workflow
            log.warn("[KAFKA-EVENT] ✗ Unexpected error publishing event for payment {}: {}",
                event.getPaymentId(), e.getMessage());
        }
    }

    // ── Builder ───────────────────────────────────────────────────────────────

    private PaymentEventMessage buildEvent(PaymentContext ctx, String eventType, String details) {
        var req = ctx.getOriginalRequest();
        return PaymentEventMessage.builder()
            .eventId(UUID.randomUUID().toString())
            .eventType(eventType)
            .paymentId(ctx.getAssignedPaymentId())
            .transactionReference(ctx.getTransactionReference())
            .sourceAccountId(req != null ? req.getSourceAccountId() : null)
            .destinationAccountId(req != null ? req.getDestinationAccountId() : null)
            .amount(req != null ? req.getAmount() : BigDecimal.ZERO)
            .currency(req != null ? req.getCurrency() : null)
            .status(ctx.getCurrentStatus() != null ? ctx.getCurrentStatus().name() : "UNKNOWN")
            .details(details)
            .eventTimestamp(LocalDateTime.now())
            .build();
    }
}
