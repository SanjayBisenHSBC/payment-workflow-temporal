package com.payment.workflow.model;

/**
 * Represents the lifecycle states of a payment through the workflow.
 *
 * State Transitions:
 *
 *  INITIATED → VALIDATED → FRAUD_CHECKED → ACCOUNTING_POSTED → COMPLETED
 *      ↓            ↓             ↓                ↓
 *   FAILED       FAILED        BLOCKED          FAILED
 *
 */
public enum PaymentStatus {

    /** Payment has been received and is being processed */
    INITIATED,

    /** Payment passed validation and derivations are complete */
    VALIDATED,

    /** Payment passed fraud screening */
    FRAUD_CHECKED,

    /** Accounting entries have been posted */
    ACCOUNTING_POSTED,

    /** Payment completed successfully and notifications sent */
    COMPLETED,

    /** Payment was blocked due to fraud suspicion */
    FRAUD_BLOCKED,

    /** Payment failed at any stage */
    FAILED
}
