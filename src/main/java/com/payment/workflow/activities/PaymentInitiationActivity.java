package com.payment.workflow.activities;

import com.payment.workflow.model.PaymentContext;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Activity Interface: Exit Point 1 — Payment Initiation
 *
 * ─────────────────────────────────────────────────────────────────
 * PURPOSE
 * ─────────────────────────────────────────────────────────────────
 * The initiation step "books" the payment into the system.
 * It assigns identifiers and timestamps before any processing begins.
 *
 * ─────────────────────────────────────────────────────────────────
 * RESPONSIBILITIES
 * ─────────────────────────────────────────────────────────────────
 * 1. Assign a unique Payment ID (if not provided)
 * 2. Record the initiation timestamp
 * 3. Generate an internal booking reference
 * 4. Set the initial status to INITIATED
 * 5. Persist the payment record to DB (in a real implementation)
 * 6. Add an audit trail entry
 *
 * ─────────────────────────────────────────────────────────────────
 * TEMPORAL ACTIVITY RULES
 * ─────────────────────────────────────────────────────────────────
 * - Activities CAN do I/O (unlike Workflows)
 * - Activities are retried on failure based on RetryOptions in Workflow
 * - Activities must be IDEMPOTENT when retried (same input → same output)
 *   Use the paymentId as an idempotency key when writing to DB.
 * - @ActivityInterface marks this for Temporal's proxy generation
 * - @ActivityMethod is optional but documents the task name explicitly
 */
@ActivityInterface
public interface PaymentInitiationActivity {

    /**
     * Initiates a payment — assigns ID, booking reference, and timestamps.
     *
     * @param context The initial workflow context with the original request
     * @return Enriched context with assigned payment ID and initiation data
     */
    @ActivityMethod
    PaymentContext initiatePayment(PaymentContext context);
}
