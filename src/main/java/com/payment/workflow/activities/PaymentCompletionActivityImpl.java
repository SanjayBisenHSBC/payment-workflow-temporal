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

/**
 * Exit Point 5 — Payment Completion & Notifications
 *
 * Changes from v1:
 *  + Persists final completion state via PaymentPersistenceService
 *  + Publishes PAYMENT_COMPLETED event to Kafka (triggers downstream notifications)
 */
@Slf4j
@Component("paymentCompletionActivity")
@RequiredArgsConstructor
public class PaymentCompletionActivityImpl implements PaymentCompletionActivity {

    private final PaymentPersistenceService persistenceService;
    private final PaymentEventProducer eventProducer;

    private static final DateTimeFormatter TXN_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    @Override
    public PaymentContext completeAndNotify(PaymentContext context) {
        log.info("[COMPLETION] Completing paymentId={}", context.getAssignedPaymentId());

        try {
            var req = context.getOriginalRequest();
            LocalDateTime completedAt = LocalDateTime.now();

            String txnRef = "TXN-" + completedAt.format(TXN_FORMAT) + "-"
                + context.getAssignedPaymentId().replace("PAY-", "");
            context.setTransactionReference(txnRef);

            boolean senderNotified  = sendNotification("SENDER",  req.getSourceAccountId(),
                String.format("Your payment of %s %s to %s has been processed. Ref: %s.",
                    req.getAmount(), req.getCurrency(), req.getDestinationAccountId(), txnRef));

            boolean receiverNotified = sendNotification("RECEIVER", req.getDestinationAccountId(),
                String.format("You received %s %s from %s. Ref: %s.",
                    context.getConvertedAmount() != null ? context.getConvertedAmount() : req.getAmount(),
                    context.getSettlementCurrency() != null ? context.getSettlementCurrency() : req.getCurrency(),
                    req.getSourceAccountId(), txnRef));

            context.setNotificationSentToSender(senderNotified);
            context.setNotificationSentToReceiver(receiverNotified);
            context.setCurrentStatus(PaymentStatus.COMPLETED);
            context.setCompletionMessage(String.format(
                "Payment completed. TXN=%s SenderNotified=%s ReceiverNotified=%s",
                txnRef, senderNotified, receiverNotified));

            context.addAuditStep(WorkflowStep.builder()
                .stepName("COMPLETION")
                .description(context.getCompletionMessage())
                .success(true)
                .executedAt(completedAt)
                .outputData(String.format("{\"txnRef\":\"%s\",\"senderNotified\":%b,\"receiverNotified\":%b}",
                    txnRef, senderNotified, receiverNotified))
                .build());

            // ── Persist + publish ──────────────────────────────────────────
            persistenceService.saveCompletion(context);
            // PAYMENT_COMPLETED is the terminal event — downstream notification
            // services subscribe to this to trigger email/SMS delivery
            eventProducer.publishCompleted(context);

            log.info("[COMPLETION] ✓ paymentId={} txnRef={}", context.getAssignedPaymentId(), txnRef);

        } catch (Exception e) {
            log.error("[COMPLETION] ✗ Failed", e);
            context.setCurrentStatus(PaymentStatus.FAILED);
            context.setFailureReason("Completion failed: " + e.getMessage());
            context.addAuditStep(WorkflowStep.builder()
                .stepName("COMPLETION").description("Failed: " + e.getMessage())
                .success(false).executedAt(LocalDateTime.now()).build());
            persistenceService.saveCompletion(context);
        }

        return context;
    }

    private boolean sendNotification(String type, String accountId, String message) {
        try {
            log.info("[NOTIFICATION] {} → {}: {}", type, accountId, message);
            Thread.sleep(50);
            return true;
        } catch (Exception e) {
            log.warn("[NOTIFICATION] ✗ Failed {} for {}: {}", type, accountId, e.getMessage());
            return false;
        }
    }
}
