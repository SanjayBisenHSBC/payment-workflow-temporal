package com.payment.workflow.persistence.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * JPA Entity — maps to the `payment_records` table.
 *
 * Represents the full lifecycle record of a single payment.
 * Fields are progressively populated as the workflow advances through
 * its 5 exit points. The `status` column reflects the latest PaymentStatus.
 *
 * CONCURRENCY NOTE:
 * The @Version field enables optimistic locking. If two threads try to
 * update the same payment_record simultaneously, only one will succeed;
 * the other will get an OptimisticLockException (safe to retry).
 */
@Entity
@Table(name = "payment_records")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Version
    private Long version;           // Optimistic locking

    @Column(name = "payment_id", nullable = false, unique = true, length = 64)
    private String paymentId;

    @Column(name = "workflow_id", nullable = false, length = 64)
    private String workflowId;

    // ── Core payment fields ────────────────────────────────────────────
    @Column(name = "source_account_id", nullable = false, length = 64)
    private String sourceAccountId;

    @Column(name = "destination_account_id", nullable = false, length = 64)
    private String destinationAccountId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 10)
    private String currency;

    @Column(name = "payment_type", nullable = false, length = 20)
    private String paymentType;

    @Column(length = 128)
    private String reference;

    @Column(length = 512)
    private String description;

    @Column(nullable = false, length = 32)
    private String status;

    @Column(name = "failure_reason", columnDefinition = "TEXT")
    private String failureReason;

    // ── Derived / validation fields ────────────────────────────────────
    @Column(name = "payment_route", length = 32)
    private String paymentRoute;

    @Column(name = "correspondent_bank", length = 128)
    private String correspondentBank;

    @Column(name = "fx_rate", precision = 18, scale = 6)
    private BigDecimal fxRate;

    @Column(name = "converted_amount", precision = 19, scale = 4)
    private BigDecimal convertedAmount;

    @Column(name = "settlement_currency", length = 10)
    private String settlementCurrency;

    @Column(name = "charge_bearer", length = 10)
    private String chargeBearer;

    @Column(name = "value_date")
    private String valueDate;

    // ── Fraud fields ────────────────────────────────────────────────────
    @Column(name = "fraud_risk_score", length = 10)
    private String fraudRiskScore;

    @Column(name = "fraud_check_details", columnDefinition = "TEXT")
    private String fraudCheckDetails;

    @Column(name = "manual_review_required")
    private Boolean manualReviewRequired;

    // ── Accounting fields ───────────────────────────────────────────────
    @Column(name = "accounting_reference", length = 64)
    private String accountingReference;

    @Column(name = "debit_ledger_entry", columnDefinition = "TEXT")
    private String debitLedgerEntry;

    @Column(name = "credit_ledger_entry", columnDefinition = "TEXT")
    private String creditLedgerEntry;

    // ── Completion fields ───────────────────────────────────────────────
    @Column(name = "transaction_reference", length = 64)
    private String transactionReference;

    @Column(name = "notification_sent_sender")
    private Boolean notificationSentSender;

    @Column(name = "notification_sent_receiver")
    private Boolean notificationSentReceiver;

    // ── Timestamps ──────────────────────────────────────────────────────
    @Column(name = "initiated_at")
    private OffsetDateTime initiatedAt;

    @Column(name = "validated_at")
    private OffsetDateTime validatedAt;

    @Column(name = "fraud_checked_at")
    private OffsetDateTime fraudCheckedAt;

    @Column(name = "accounting_posted_at")
    private OffsetDateTime accountingPostedAt;

    @Column(name = "completed_at")
    private OffsetDateTime completedAt;

    @Column(name = "updated_at")
    private OffsetDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        if (initiatedAt == null) initiatedAt = OffsetDateTime.now();
        updatedAt = OffsetDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = OffsetDateTime.now();
    }
}
