package com.payment.workflow.workflow;

import com.payment.workflow.model.PaymentContext;
import com.payment.workflow.model.PaymentRequest;
import com.payment.workflow.model.PaymentResult;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Temporal Workflow Interface for the Payment Processing Pipeline.
 *
 * ─────────────────────────────────────────────────────────────────
 * WHAT IS A TEMPORAL WORKFLOW?
 * ─────────────────────────────────────────────────────────────────
 * A Workflow in Temporal is a durable function that:
 *  - Survives server/process restarts (state is persisted in Temporal Server)
 *  - Automatically retries on failures
 *  - Can run for seconds or years
 *  - Has a deterministic execution model (no direct I/O inside workflow code)
 *
 * The @WorkflowInterface annotation marks this as a Temporal workflow.
 * Temporal uses this interface to generate a type-safe client stub.
 *
 * ─────────────────────────────────────────────────────────────────
 * WORKFLOW vs ACTIVITY
 * ─────────────────────────────────────────────────────────────────
 * - Workflow = Orchestrator (coordinates the steps, must be deterministic)
 * - Activity  = Individual step (can do I/O, DB calls, HTTP calls, etc.)
 *
 * ─────────────────────────────────────────────────────────────────
 * EXIT POINTS (Activities):
 * ─────────────────────────────────────────────────────────────────
 *  1. Payment Initiation           → Assigns ID, timestamps, references
 *  2. Payment Validation & Derivation → Validates rules, derives routing/FX
 *  3. Fraud Check                  → Scores and screens the payment
 *  4. Accounting                   → Posts debit/credit ledger entries
 *  5. Payment Completion & Notify  → Marks complete, sends notifications
 */
@WorkflowInterface
public interface PaymentWorkflow {

    /**
     * Main workflow method — executes the full payment pipeline.
     *
     * Called once per payment. Temporal will replay this method
     * from the event history if the worker restarts mid-execution.
     *
     * @param request The payment request from the REST API
     * @return Final result with status, transaction ref, and audit trail
     */
    @WorkflowMethod
    PaymentResult processPayment(PaymentRequest request);

    /**
     * Query method to inspect the current state of the workflow.
     *
     * Queries are read-only and do not alter workflow state.
     * They can be called at any point during workflow execution
     * and return the live, in-memory state.
     *
     * @return Current PaymentContext (full intermediate state)
     */
    @QueryMethod
    PaymentContext getCurrentStatus();
}
