package com.payment.workflow.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Mutable context object that flows through the entire payment workflow.
 *
 * Each activity reads from and enriches this context. It accumulates
 * all derived data, validation results, and audit entries as the
 * payment progresses through each step.
 *
 * IMPORTANT: This must be serializable by Temporal's data converter
 * (Jackson JSON by default). All fields must have no-arg constructor
 * and public getters/setters (handled by Lombok @Data).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentContext {

    // ─── Original Request ─────────────────────────────────────────────
    private PaymentRequest originalRequest;

    // ─── Status Tracking ──────────────────────────────────────────────
    private PaymentStatus currentStatus;
    private String failureReason;

    // ─── Step 1: Initiation ───────────────────────────────────────────
    private String assignedPaymentId;
    private LocalDateTime initiationTime;
    private String initiationReference;

    // ─── Step 2: Validation & Derivations ─────────────────────────────
    private boolean validationPassed;
    private String validationErrors;
    private String paymentRoute;           // Derived routing path
    private String correspondentBank;      // Derived for cross-border
    private BigDecimal fxRate;             // FX rate if currency conversion needed
    private BigDecimal convertedAmount;    // Amount in settlement currency
    private String settlementCurrency;     // Target settlement currency
    private String chargeBearer;           // WHO bears charges: OUR/BEN/SHA
    private String valueDate;              // Derived value/settlement date

    // ─── Step 3: Fraud Check ──────────────────────────────────────────
    private boolean fraudCheckPassed;
    private String fraudRiskScore;         // e.g., "LOW", "MEDIUM", "HIGH"
    private String fraudCheckDetails;
    private boolean manualReviewRequired;

    // ─── Step 4: Accounting ───────────────────────────────────────────
    private String debitLedgerEntry;
    private String creditLedgerEntry;
    private String accountingReference;
    private boolean accountingPosted;

    // ─── Step 5: Completion & Notifications ───────────────────────────
    private String transactionReference;
    private boolean notificationSentToSender;
    private boolean notificationSentToReceiver;
    private String completionMessage;

    // ─── Audit Trail ──────────────────────────────────────────────────
    @Builder.Default
    private List<WorkflowStep> auditTrail = new ArrayList<>();

    public void addAuditStep(WorkflowStep step) {
        if (this.auditTrail == null) {
            this.auditTrail = new ArrayList<>();
        }
        this.auditTrail.add(step);
    }
}
