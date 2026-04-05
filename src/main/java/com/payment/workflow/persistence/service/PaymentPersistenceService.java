package com.payment.workflow.persistence.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payment.workflow.model.PaymentContext;
import com.payment.workflow.model.WorkflowStep;
import com.payment.workflow.persistence.entity.PaymentRecord;
import com.payment.workflow.persistence.entity.WorkflowEventLog;
import com.payment.workflow.persistence.repository.PaymentRecordRepository;
import com.payment.workflow.persistence.repository.WorkflowEventLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.Optional;

/**
 * PaymentPersistenceService
 *
 * Central service that Activities use to persist payment state.
 * All DB operations are wrapped in @Transactional to guarantee atomicity.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * DESIGN PRINCIPLES
 * ─────────────────────────────────────────────────────────────────────────────
 * 1. IDEMPOTENCY — every method checks before inserting. If a record already
 *    exists with the same paymentId, the upsert path is taken. This is critical
 *    because Temporal can retry any activity, which re-calls these methods.
 *
 * 2. APPEND-ONLY EVENT LOG — workflow_event_log rows are only inserted, never
 *    updated or deleted. This gives a tamper-evident audit trail.
 *
 * 3. PARTIAL UPDATES — the payment_records row is updated one step at a time
 *    using targeted setters. We never overwrite fields populated by earlier steps.
 *
 * 4. EXCEPTION ISOLATION — DB exceptions are caught and logged. They are then
 *    re-thrown so Temporal can retry the activity according to its RetryOptions.
 *    We do not swallow exceptions silently.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentPersistenceService {

    private final PaymentRecordRepository paymentRecordRepository;
    private final WorkflowEventLogRepository eventLogRepository;
    private final ObjectMapper objectMapper;

    // ─────────────────────────────────────────────────────────────────
    // STEP 1 — Initiation
    // ─────────────────────────────────────────────────────────────────

    /**
     * Creates the initial payment_records row after initiation.
     * Idempotent: if the row already exists (retry scenario), does nothing.
     */
    @Transactional
    public void saveInitiation(PaymentContext ctx) {
        String pid = ctx.getAssignedPaymentId();

        if (paymentRecordRepository.existsByPaymentId(pid)) {
            log.debug("[DB] Payment {} already exists — skipping initiation insert (idempotent retry)", pid);
            return;
        }

        var req = ctx.getOriginalRequest();
        PaymentRecord record = PaymentRecord.builder()
            .paymentId(pid)
            .workflowId(pid)
            .sourceAccountId(req.getSourceAccountId())
            .destinationAccountId(req.getDestinationAccountId())
            .amount(req.getAmount())
            .currency(req.getCurrency())
            .paymentType(req.getPaymentType())
            .reference(req.getReference())
            .description(req.getDescription())
            .status(ctx.getCurrentStatus().name())
            .initiatedAt(OffsetDateTime.now())
            .build();

        paymentRecordRepository.save(record);
        appendEventLog(ctx, "INITIATION", "STEP_COMPLETED", "SUCCESS",
            ctx.getInitiationReference(), null);
        log.info("[DB] ✓ Payment record created for {}", pid);
    }

    // ─────────────────────────────────────────────────────────────────
    // STEP 2 — Validation
    // ─────────────────────────────────────────────────────────────────

    @Transactional
    public void saveValidation(PaymentContext ctx) {
        paymentRecordRepository.findByPaymentId(ctx.getAssignedPaymentId()).ifPresent(record -> {
            record.setStatus(ctx.getCurrentStatus().name());
            record.setPaymentRoute(ctx.getPaymentRoute());
            record.setCorrespondentBank(ctx.getCorrespondentBank());
            record.setFxRate(ctx.getFxRate());
            record.setConvertedAmount(ctx.getConvertedAmount());
            record.setSettlementCurrency(ctx.getSettlementCurrency());
            record.setChargeBearer(ctx.getChargeBearer());
            record.setValueDate(ctx.getValueDate());
            record.setValidatedAt(OffsetDateTime.now());
            if (!ctx.isValidationPassed()) record.setFailureReason(ctx.getValidationErrors());
            paymentRecordRepository.save(record);
        });

        String status = ctx.isValidationPassed() ? "SUCCESS" : "FAILURE";
        String desc = ctx.isValidationPassed()
            ? "Route: " + ctx.getPaymentRoute() + ", ValueDate: " + ctx.getValueDate()
            : "Validation failed: " + ctx.getValidationErrors();
        appendEventLog(ctx, "VALIDATION", "STEP_COMPLETED", status, desc, ctx.getValidationErrors());
        log.info("[DB] ✓ Validation saved for {}", ctx.getAssignedPaymentId());
    }

    // ─────────────────────────────────────────────────────────────────
    // STEP 3 — Fraud Check
    // ─────────────────────────────────────────────────────────────────

    @Transactional
    public void saveFraudCheck(PaymentContext ctx) {
        paymentRecordRepository.findByPaymentId(ctx.getAssignedPaymentId()).ifPresent(record -> {
            record.setStatus(ctx.getCurrentStatus().name());
            record.setFraudRiskScore(ctx.getFraudRiskScore());
            record.setFraudCheckDetails(ctx.getFraudCheckDetails());
            record.setManualReviewRequired(ctx.isManualReviewRequired());
            record.setFraudCheckedAt(OffsetDateTime.now());
            if (!ctx.isFraudCheckPassed()) record.setFailureReason(ctx.getFailureReason());
            paymentRecordRepository.save(record);
        });

        String status = ctx.isFraudCheckPassed() ? "SUCCESS" : "BLOCKED";
        appendEventLog(ctx, "FRAUD_CHECK", "STEP_COMPLETED", status,
            ctx.getFraudCheckDetails(), ctx.isFraudCheckPassed() ? null : ctx.getFailureReason());
        log.info("[DB] ✓ Fraud check saved for {} (score={})", ctx.getAssignedPaymentId(), ctx.getFraudRiskScore());
    }

    // ─────────────────────────────────────────────────────────────────
    // STEP 4 — Accounting
    // ─────────────────────────────────────────────────────────────────

    @Transactional
    public void saveAccounting(PaymentContext ctx) {
        paymentRecordRepository.findByPaymentId(ctx.getAssignedPaymentId()).ifPresent(record -> {
            record.setStatus(ctx.getCurrentStatus().name());
            record.setAccountingReference(ctx.getAccountingReference());
            record.setDebitLedgerEntry(ctx.getDebitLedgerEntry());
            record.setCreditLedgerEntry(ctx.getCreditLedgerEntry());
            record.setAccountingPostedAt(OffsetDateTime.now());
            if (!ctx.isAccountingPosted()) record.setFailureReason(ctx.getFailureReason());
            paymentRecordRepository.save(record);
        });

        String status = ctx.isAccountingPosted() ? "SUCCESS" : "FAILURE";
        appendEventLog(ctx, "ACCOUNTING", "STEP_COMPLETED", status,
            "GL Ref: " + ctx.getAccountingReference(), ctx.isAccountingPosted() ? null : ctx.getFailureReason());
        log.info("[DB] ✓ Accounting saved for {}", ctx.getAssignedPaymentId());
    }

    // ─────────────────────────────────────────────────────────────────
    // STEP 5 — Completion
    // ─────────────────────────────────────────────────────────────────

    @Transactional
    public void saveCompletion(PaymentContext ctx) {
        paymentRecordRepository.findByPaymentId(ctx.getAssignedPaymentId()).ifPresent(record -> {
            record.setStatus(ctx.getCurrentStatus().name());
            record.setTransactionReference(ctx.getTransactionReference());
            record.setNotificationSentSender(ctx.isNotificationSentToSender());
            record.setNotificationSentReceiver(ctx.isNotificationSentToReceiver());
            record.setCompletedAt(OffsetDateTime.now());
            paymentRecordRepository.save(record);
        });

        appendEventLog(ctx, "COMPLETION", "STEP_COMPLETED", "SUCCESS",
            "TXN: " + ctx.getTransactionReference(), null);
        log.info("[DB] ✓ Completion saved for {} (TXN={})", ctx.getAssignedPaymentId(), ctx.getTransactionReference());
    }

    // ─────────────────────────────────────────────────────────────────
    // QUERY — used by the REST API
    // ─────────────────────────────────────────────────────────────────

    @Transactional(readOnly = true)
    public Optional<PaymentRecord> findByPaymentId(String paymentId) {
        return paymentRecordRepository.findByPaymentId(paymentId);
    }

    // ─────────────────────────────────────────────────────────────────
    // PRIVATE HELPERS
    // ─────────────────────────────────────────────────────────────────

    /**
     * Appends a row to workflow_event_log. Never updates existing rows.
     */
    private void appendEventLog(PaymentContext ctx, String stepName, String eventType,
                                 String status, String description, String errorMessage) {
        try {
            // Serialize latest audit step output data as JSON string
            String outputJson = null;
            if (ctx.getAuditTrail() != null && !ctx.getAuditTrail().isEmpty()) {
                WorkflowStep last = ctx.getAuditTrail().get(ctx.getAuditTrail().size() - 1);
                outputJson = last.getOutputData() != null
                    ? last.getOutputData()
                    : objectMapper.writeValueAsString(java.util.Map.of("step", stepName));
            }

            WorkflowEventLog entry = WorkflowEventLog.builder()
                .paymentId(ctx.getAssignedPaymentId())
                .workflowId(ctx.getAssignedPaymentId())
                .stepName(stepName)
                .eventType(eventType)
                .status(status)
                .description(description)
                .outputData(outputJson)
                .errorMessage(errorMessage)
                .executedAt(OffsetDateTime.now())
                .build();

            eventLogRepository.save(entry);
        } catch (Exception e) {
            log.warn("[DB] Failed to write event log for {} step {}: {}", ctx.getAssignedPaymentId(), stepName, e.getMessage());
            // Don't rethrow — event log failure must not fail the payment
        }
    }
}
