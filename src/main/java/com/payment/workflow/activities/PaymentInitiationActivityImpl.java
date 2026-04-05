package com.payment.workflow.activities;

import com.payment.workflow.kafka.producer.PaymentEventProducer;
import com.payment.workflow.model.PaymentContext;
import com.payment.workflow.model.PaymentStatus;
import com.payment.workflow.model.WorkflowStep;
import com.payment.workflow.persistence.service.PaymentPersistenceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * Exit Point 1 — Payment Initiation
 *
 * Changes from v1:
 *  + Persists the initial payment_records row via PaymentPersistenceService
 *  + Appends a row to workflow_event_log
 *  + Publishes PAYMENT_INITIATED event to Kafka payment.events topic
 */
@Slf4j
@Component("paymentInitiationActivity")
@RequiredArgsConstructor
public class PaymentInitiationActivityImpl implements PaymentInitiationActivity {

    private final PaymentPersistenceService persistenceService;
    private final PaymentEventProducer eventProducer;

    private static final DateTimeFormatter REF_FORMAT =
        DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    @Override
    public PaymentContext initiatePayment(PaymentContext context) {
        log.info("[INITIATION] Starting for request: {}",
            context.getOriginalRequest().getPaymentId());

        try {
            String paymentId = (context.getOriginalRequest().getPaymentId() != null
                && !context.getOriginalRequest().getPaymentId().isBlank())
                ? context.getOriginalRequest().getPaymentId()
                : "PAY-" + UUID.randomUUID().toString().substring(0, 12).toUpperCase();

            LocalDateTime initiationTime = LocalDateTime.now();
            String bookingRef = "BKG-" + initiationTime.format(REF_FORMAT)
                + "-" + paymentId.substring(Math.max(0, paymentId.length() - 6));

            log.info("[INITIATION] Assigned paymentId={} bookingRef={}", paymentId, bookingRef);

            context.setAssignedPaymentId(paymentId);
            context.setInitiationTime(initiationTime);
            context.setInitiationReference(bookingRef);
            context.setCurrentStatus(PaymentStatus.INITIATED);

            context.addAuditStep(WorkflowStep.builder()
                .stepName("INITIATION")
                .description(String.format("Payment initiated. ID=%s, Amount=%s %s, %s→%s",
                    paymentId, context.getOriginalRequest().getAmount(),
                    context.getOriginalRequest().getCurrency(),
                    context.getOriginalRequest().getSourceAccountId(),
                    context.getOriginalRequest().getDestinationAccountId()))
                .success(true)
                .executedAt(initiationTime)
                .outputData(String.format("{\"paymentId\":\"%s\",\"bookingRef\":\"%s\"}", paymentId, bookingRef))
                .build());

            // ── Persist to DB ────────────────────────────────────────────────
            persistenceService.saveInitiation(context);

            // ── Publish Kafka event ──────────────────────────────────────────
            eventProducer.publishInitiated(context);

            log.info("[INITIATION] ✓ Complete for paymentId={}", paymentId);

        } catch (Exception e) {
            log.error("[INITIATION] ✗ Failed", e);
            context.setCurrentStatus(PaymentStatus.FAILED);
            context.setFailureReason("Initiation failed: " + e.getMessage());
            context.addAuditStep(WorkflowStep.builder()
                .stepName("INITIATION").description("Failed: " + e.getMessage())
                .success(false).executedAt(LocalDateTime.now()).build());
        }

        return context;
    }
}
