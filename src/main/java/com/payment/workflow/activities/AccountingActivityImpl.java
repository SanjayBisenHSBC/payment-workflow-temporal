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
 * Exit Point 4 — Accounting
 *
 * Changes from v1:
 *  + Persists GL entry details via PaymentPersistenceService
 *  + Publishes ACCOUNTING_POSTED event to Kafka
 */
@Slf4j
@Component("accountingActivity")
@RequiredArgsConstructor
public class AccountingActivityImpl implements AccountingActivity {

    private final PaymentPersistenceService persistenceService;
    private final PaymentEventProducer eventProducer;

    private static final DateTimeFormatter REF_FORMAT =
        DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");

    @Override
    public PaymentContext postAccounting(PaymentContext context) {
        log.info("[ACCOUNTING] Posting GL entries for paymentId={}", context.getAssignedPaymentId());

        try {
            var req = context.getOriginalRequest();
            LocalDateTime now = LocalDateTime.now();

            String accountingRef = "GL-" + now.format(REF_FORMAT) + "-"
                + UUID.randomUUID().toString().substring(0, 6).toUpperCase();

            String debitEntry = String.format(
                "DEBIT | Account=%s | Amount=%s %s | Ref=%s | ValueDate=%s | To=%s",
                req.getSourceAccountId(), req.getAmount(), req.getCurrency(),
                accountingRef, context.getValueDate(), req.getDestinationAccountId());

            String creditEntry = String.format(
                "CREDIT | Account=%s | Amount=%s %s | Ref=%s | ValueDate=%s | From=%s",
                req.getDestinationAccountId(),
                context.getConvertedAmount() != null ? context.getConvertedAmount() : req.getAmount(),
                context.getSettlementCurrency() != null ? context.getSettlementCurrency() : req.getCurrency(),
                accountingRef, context.getValueDate(), req.getSourceAccountId());

            log.info("[ACCOUNTING] Debit:  {}", debitEntry);
            log.info("[ACCOUNTING] Credit: {}", creditEntry);

            // Simulate GL system call
            Thread.sleep(100);

            context.setDebitLedgerEntry(debitEntry);
            context.setCreditLedgerEntry(creditEntry);
            context.setAccountingReference(accountingRef);
            context.setAccountingPosted(true);
            context.setCurrentStatus(PaymentStatus.ACCOUNTING_POSTED);

            context.addAuditStep(WorkflowStep.builder()
                .stepName("ACCOUNTING")
                .description("GL posted. Ref=" + accountingRef)
                .success(true)
                .executedAt(now)
                .outputData(String.format("{\"accountingRef\":\"%s\",\"debit\":\"%s\",\"credit\":\"%s\"}",
                    accountingRef, req.getSourceAccountId(), req.getDestinationAccountId()))
                .build());

            // ── Persist + publish ──────────────────────────────────────────
            persistenceService.saveAccounting(context);
            eventProducer.publishAccountingPosted(context);

            log.info("[ACCOUNTING] ✓ Complete for paymentId={} ref={}", context.getAssignedPaymentId(), accountingRef);

        } catch (Exception e) {
            log.error("[ACCOUNTING] ✗ Failed", e);
            context.setAccountingPosted(false);
            context.setCurrentStatus(PaymentStatus.FAILED);
            context.setFailureReason("Accounting failed: " + e.getMessage());
            context.addAuditStep(WorkflowStep.builder()
                .stepName("ACCOUNTING").description("Failed: " + e.getMessage())
                .success(false).executedAt(LocalDateTime.now()).build());
            persistenceService.saveAccounting(context);
        }

        return context;
    }
}
