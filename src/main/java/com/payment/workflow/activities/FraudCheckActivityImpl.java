package com.payment.workflow.activities;

import com.payment.workflow.kafka.consumer.FraudResponseConsumer;
import com.payment.workflow.kafka.model.FraudCheckRequestMessage;
import com.payment.workflow.kafka.model.FraudCheckResponse;
import com.payment.workflow.kafka.producer.FraudCheckProducer;
import com.payment.workflow.kafka.producer.PaymentEventProducer;
import com.payment.workflow.model.PaymentContext;
import com.payment.workflow.model.PaymentStatus;
import com.payment.workflow.model.WorkflowStep;
import com.payment.workflow.persistence.service.PaymentPersistenceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Exit Point 3 — Fraud Check (Kafka-integrated)
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * KAFKA INTEGRATION FLOW
 * ─────────────────────────────────────────────────────────────────────────────
 *
 *  1. Generate a correlationId (UUID).
 *  2. Register a CompletableFuture in FraudResponseConsumer.pendingResponses.
 *  3. Publish FraudCheckRequestMessage to payment.fraud.request.
 *  4. Wait (block) on the future for up to `fraud-response-timeout-seconds`.
 *  5a. If response arrives in time → use the external fraud engine's decision.
 *  5b. If timeout → fall back to internal rule-based scoring (fail-open).
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * TEMPORAL ACTIVITY THREADING NOTE
 * ─────────────────────────────────────────────────────────────────────────────
 * Temporal activities run on a thread-pool managed by the Worker.
 * Calling future.get() here blocks that worker thread for up to `timeout` seconds.
 * Configure your WorkerOptions.maxConcurrentActivityExecutionSize to be large
 * enough to accommodate blocked fraud-check threads without starving other activities.
 * A recommended pattern for high-throughput systems is to use Temporal's
 * async activity completion API instead — but blocking is simpler and fine
 * for moderate payment volumes.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * FALLBACK STRATEGY
 * ─────────────────────────────────────────────────────────────────────────────
 * If the Kafka fraud system is unavailable or times out, we fall back to
 * the internal rule engine (same rules as the original FraudCheckActivityImpl).
 * This is a "fail-open with audit" strategy — payments are not blocked solely
 * due to infrastructure unavailability, but the fallback is logged prominently.
 */
@Slf4j
@Component("fraudCheckActivity")
@RequiredArgsConstructor
public class FraudCheckActivityImpl implements FraudCheckActivity {

    private final FraudCheckProducer fraudCheckProducer;
    private final FraudResponseConsumer fraudResponseConsumer;
    private final PaymentPersistenceService persistenceService;
    private final PaymentEventProducer eventProducer;

    @Value("${app.kafka.consumer.fraud-response-timeout-seconds:30}")
    private long fraudResponseTimeoutSeconds;

    private static final Set<String> HIGH_RISK_ACCOUNTS = Set.of(
        "ACC-BLOCKED-001", "ACC-SANCTIONED-002", "ACC-SUSPICIOUS-003");
    private static final BigDecimal HIGH_VALUE_THRESHOLD   = new BigDecimal("50000.00");
    private static final BigDecimal MEDIUM_VALUE_THRESHOLD = new BigDecimal("10000.00");

    @Override
    public PaymentContext performFraudCheck(PaymentContext context) {
        log.info("[FRAUD_CHECK] Starting for paymentId={}", context.getAssignedPaymentId());

        try {
            FraudCheckResponse response = requestFraudCheckViaKafka(context);

            if (response != null) {
                applyExternalFraudDecision(context, response);
            } else {
                log.warn("[FRAUD_CHECK] Kafka fraud system unavailable — falling back to internal rules for {}",
                    context.getAssignedPaymentId());
                applyInternalFraudRules(context);
            }

            // ── Persist + publish ──────────────────────────────────────────
            persistenceService.saveFraudCheck(context);
            if (context.isFraudCheckPassed()) {
                eventProducer.publishFraudCleared(context);
            } else {
                eventProducer.publishFraudBlocked(context);
            }

        } catch (Exception e) {
            log.error("[FRAUD_CHECK] ✗ Unexpected error — blocking payment (fail-safe)", e);
            context.setFraudCheckPassed(false);
            context.setCurrentStatus(PaymentStatus.FAILED);
            context.setFailureReason("Fraud check system error: " + e.getMessage());
            context.addAuditStep(WorkflowStep.builder()
                .stepName("FRAUD_CHECK").description("System error: " + e.getMessage())
                .success(false).executedAt(LocalDateTime.now()).build());
            persistenceService.saveFraudCheck(context);
        }

        return context;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // KAFKA PATH
    // ─────────────────────────────────────────────────────────────────────────

    private FraudCheckResponse requestFraudCheckViaKafka(PaymentContext context) {
        String correlationId = UUID.randomUUID().toString();
        var req = context.getOriginalRequest();

        FraudCheckRequestMessage message = FraudCheckRequestMessage.builder()
            .correlationId(correlationId)
            .paymentId(context.getAssignedPaymentId())
            .sourceAccountId(req.getSourceAccountId())
            .destinationAccountId(req.getDestinationAccountId())
            .amount(req.getAmount())
            .currency(req.getCurrency())
            .paymentType(req.getPaymentType())
            .paymentRoute(context.getPaymentRoute())
            .senderMetadata(req.getSenderMetadata())
            .requestTimestamp(LocalDateTime.now())
            .build();

        try {
            // Register the future BEFORE publishing (avoids race condition where
            // response arrives before we register)
            fraudResponseConsumer.registerPendingRequest(correlationId);

            // Publish to Kafka
            fraudCheckProducer.sendFraudRequest(message).get(5, java.util.concurrent.TimeUnit.SECONDS);
            log.info("[FRAUD_CHECK] Request sent to Kafka. correlationId={}", correlationId);

            // Wait for the fraud system to respond
            return fraudResponseConsumer.waitForResponse(correlationId, fraudResponseTimeoutSeconds);

        } catch (Exception e) {
            log.warn("[FRAUD_CHECK] Kafka path failed ({}), will use internal rules", e.getMessage());
            return null;
        }
    }

    private void applyExternalFraudDecision(PaymentContext context, FraudCheckResponse response) {
        boolean passed = !response.isBlocked();
        String riskLevel = response.getRiskLevel();
        String details = String.format(
            "External fraud engine. Score=%d, Level=%s, Details=[%s]",
            response.getRiskScore(), riskLevel, response.getRiskDetails());

        setFraudResult(context, passed, riskLevel, details, response.isManualReviewRequired());
        log.info("[FRAUD_CHECK] External decision applied: passed={} risk={}", passed, riskLevel);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // INTERNAL FALLBACK RULES
    // ─────────────────────────────────────────────────────────────────────────

    private void applyInternalFraudRules(PaymentContext context) {
        var req = context.getOriginalRequest();
        List<String> factors = new ArrayList<>();
        int score = 0;

        if (HIGH_RISK_ACCOUNTS.contains(req.getSourceAccountId())
            || HIGH_RISK_ACCOUNTS.contains(req.getDestinationAccountId())) {
            score += 100;
            factors.add("SANCTIONS: Account on blocked list");
        }
        if (req.getAmount() != null && req.getAmount().compareTo(HIGH_VALUE_THRESHOLD) > 0) {
            score += 40;
            factors.add("HIGH_VALUE: > $50,000");
        } else if (req.getAmount() != null && req.getAmount().compareTo(MEDIUM_VALUE_THRESHOLD) > 0) {
            score += 20;
            factors.add("MEDIUM_VALUE: > $10,000");
        }
        if ("CROSS_BORDER".equals(context.getPaymentRoute())) {
            score += 10;
            factors.add("CROSS_BORDER");
        }
        if (req.getAmount() != null) {
            BigDecimal amount = req.getAmount();
            if (amount.compareTo(new BigDecimal("9990")) > 0
                && amount.compareTo(new BigDecimal("10000")) < 0) {
                score += 30;
                factors.add("STRUCTURING: Amount near $10,000 threshold");
            }
        }
        if (context.getAssignedPaymentId() != null
            && context.getAssignedPaymentId().contains("VEL")) {
            score += 35;
            factors.add("VELOCITY: High transaction frequency");
        }

        String riskLevel = score >= 70 ? "HIGH" : score >= 30 ? "MEDIUM" : "LOW";
        boolean passed = score < 70;
        boolean manualReview = score >= 30;
        String details = String.format("Internal rules (fallback). Score=%d/100, Level=%s, Factors=[%s]",
            score, riskLevel, factors.isEmpty() ? "NONE" : String.join(", ", factors));

        setFraudResult(context, passed, riskLevel, details, manualReview);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SHARED RESULT SETTER
    // ─────────────────────────────────────────────────────────────────────────

    private void setFraudResult(PaymentContext context, boolean passed, String riskLevel,
                                 String details, boolean manualReview) {
        context.setFraudCheckPassed(passed);
        context.setFraudRiskScore(riskLevel);
        context.setFraudCheckDetails(details);
        context.setManualReviewRequired(manualReview);

        if (passed) {
            context.setCurrentStatus(PaymentStatus.FRAUD_CHECKED);
            log.info("[FRAUD_CHECK] ✓ Cleared. paymentId={} risk={}", context.getAssignedPaymentId(), riskLevel);
        } else {
            context.setCurrentStatus(PaymentStatus.FRAUD_BLOCKED);
            context.setFailureReason("Payment blocked. " + details);
            log.warn("[FRAUD_CHECK] ✗ BLOCKED. paymentId={} risk={}", context.getAssignedPaymentId(), riskLevel);
        }

        context.addAuditStep(WorkflowStep.builder()
            .stepName("FRAUD_CHECK")
            .description((passed ? "PASSED" : "BLOCKED") + ". " + details
                + (manualReview ? " — MANUAL REVIEW REQUIRED" : ""))
            .success(passed)
            .executedAt(LocalDateTime.now())
            .outputData(details)
            .build());
    }
}
