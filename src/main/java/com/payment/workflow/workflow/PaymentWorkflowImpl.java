package com.payment.workflow.workflow;

import com.payment.workflow.activities.*;
import com.payment.workflow.decision.FraudDecision;
import com.payment.workflow.decision.PaymentContextMerger;
import com.payment.workflow.decision.PaymentDecisionEngine;
import com.payment.workflow.decision.RouteType;
import com.payment.workflow.model.*;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Implementation of the Payment Processing Workflow.
 *
 * ─────────────────────────────────────────────────────────────────
 * CRITICAL RULES FOR WORKFLOW CODE (READ THIS CAREFULLY)
 * ─────────────────────────────────────────────────────────────────
 *
 * 1. DETERMINISM: Workflow code MUST be deterministic.
 *    - DO NOT use System.currentTimeMillis() → use Workflow.currentTimeMillis()
 *    - DO NOT use Random → use Workflow.newRandom()
 *    - DO NOT use UUID.randomUUID() → use Workflow.randomUUID()
 *    - DO NOT sleep with Thread.sleep() → use Workflow.sleep()
 *    - DO NOT call external APIs/DB directly → wrap in Activities
 *
 * 2. WHY DETERMINISM? Temporal replays workflow history to reconstruct
 *    state after crashes. Non-deterministic code produces different
 *    results on replay, causing "non-deterministic workflow error".
 *
 * 3. ACTIVITIES are the ONLY place to do I/O (HTTP, DB, file, etc.)
 *
 * 4. DECISIONS AND DATA MERGING ARE NOT I/O, but they're split into
 *    two distinct responsibilities, each with its own class:
 *      - PaymentDecisionEngine: "what should happen next?" — branching
 *        only. Never mutates PaymentContext.
 *      - PaymentContextMerger: "reconcile this response into state" —
 *        called after every exit point. Never branches on business
 *        rules; it only takes decision outputs as plain parameters
 *        when it needs them (see mergeForAccounting).
 *    Both are plain objects (not ActivityStubs), pure, and therefore
 *    safe to call directly from workflow code.
 *
 * ─────────────────────────────────────────────────────────────────
 * HOW TEMPORAL HANDLES FAILURES
 * ─────────────────────────────────────────────────────────────────
 * Each Activity stub has RetryOptions configured below. If an Activity
 * throws an exception, Temporal will automatically retry it according
 * to the retry policy — no manual retry code needed.
 *
 * ─────────────────────────────────────────────────────────────────
 * TASK QUEUES
 * ─────────────────────────────────────────────────────────────────
 * The worker polls "payment-task-queue" for tasks. Both the workflow
 * and its activities are registered on this same queue in this example.
 * In production you may use separate queues per activity type.
 */
public class PaymentWorkflowImpl implements PaymentWorkflow {

    // ─── Activity Stub Configuration ────────────────────────────────────
    // Stubs are proxy objects — calling methods on them schedules Activity
    // Tasks in Temporal Server, which workers pick up and execute.

    private final PaymentInitiationActivity initiationActivity =
        Workflow.newActivityStub(PaymentInitiationActivity.class, buildActivityOptions(
            Duration.ofSeconds(30),
            3,
            Duration.ofSeconds(2)
        ));

    private final PaymentValidationActivity validationActivity =
        Workflow.newActivityStub(PaymentValidationActivity.class, buildActivityOptions(
            Duration.ofSeconds(60),
            3,
            Duration.ofSeconds(2)
        ));

    private final FraudCheckActivity fraudCheckActivity =
        Workflow.newActivityStub(FraudCheckActivity.class, buildActivityOptions(
            Duration.ofSeconds(120),   // Fraud checks may take longer
            2,                          // Fewer retries — fraud decisions shouldn't auto-retry aggressively
            Duration.ofSeconds(5)
        ));

    private final AccountingActivity accountingActivity =
        Workflow.newActivityStub(AccountingActivity.class, buildActivityOptions(
            Duration.ofSeconds(60),
            5,                          // More retries — accounting must succeed
            Duration.ofSeconds(3)
        ));

    private final PaymentCompletionActivity completionActivity =
        Workflow.newActivityStub(PaymentCompletionActivity.class, buildActivityOptions(
            Duration.ofSeconds(60),
            3,
            Duration.ofSeconds(2)
        ));

    // NEW — only invoked conditionally for cross-border routes.
    private final ComplianceReportingActivity complianceReportingActivity =
        Workflow.newActivityStub(ComplianceReportingActivity.class, buildActivityOptions(
            Duration.ofSeconds(60), 3, Duration.ofSeconds(2)
        ));

    // ─── Decisions vs. Merging — two separate, single-purpose helpers ───
    private final PaymentDecisionEngine decisionEngine = new PaymentDecisionEngine();
    private final PaymentContextMerger contextMerger = new PaymentContextMerger();

    // ─── Workflow State ──────────────────────────────────────────────────
    // This field persists across replays because Temporal serializes it
    private PaymentContext context;

    // ─────────────────────────────────────────────────────────────────────
    // MAIN WORKFLOW METHOD
    // ─────────────────────────────────────────────────────────────────────

    @Override
    public PaymentResult processPayment(PaymentRequest request) {

        // Initialize context — this is the "state bag" for the entire workflow
        context = PaymentContext.builder()
            .originalRequest(request)
            .currentStatus(PaymentStatus.INITIATED)
            .auditTrail(new java.util.ArrayList<>())
            .build();

        // ── EXIT POINT 1: Payment Initiation ──────────────────────────
        PaymentContext initiationResult = initiationActivity.initiatePayment(context);
        context = contextMerger.mergeInitiationResult(context, initiationResult);

        if (context.getCurrentStatus() == PaymentStatus.FAILED) {
            return buildResult(context);
        }

        // ── EXIT POINT 2: Payment Validation & Derivations ────────────
        PaymentContext validationResult = validationActivity.validateAndDerive(context);
        context = contextMerger.mergeValidationResult(context, validationResult);

        if (!context.isValidationPassed()) {
            context.setCurrentStatus(PaymentStatus.FAILED);
            return buildResult(context);
        }

        // ── DECISION POINT: Route classification (decision engine only) ──
        RouteType route = decisionEngine.classifyRoute(context);
        context.setRouteType(route);

        // ── EXIT POINT 3: Fraud Check ─────────────────────────────────
        PaymentContext fraudResult = fraudCheckActivity.performFraudCheck(context);
        context = contextMerger.mergeFraudCheckResult(context, fraudResult);

        // ── DECISION POINT: Multi-way fraud branching (decision engine only) ──
        FraudDecision fraudDecision = decisionEngine.evaluateFraudRisk(context, route);

        switch (fraudDecision) {
            case BLOCK:
                context.setCurrentStatus(PaymentStatus.FRAUD_BLOCKED);
                return buildResult(context);

            case MANUAL_REVIEW:
                context.setCurrentStatus(PaymentStatus.PENDING_REVIEW);
                Workflow.await(() -> context.isReviewDecisionReceived());
                if (!context.isReviewApproved()) {
                    context.setCurrentStatus(PaymentStatus.FRAUD_BLOCKED);
                    return buildResult(context);
                }
                break;

            case PROCEED:
            default:
                break;
        }

        // ── MERGE: fold decisions into the fields accounting needs ───────
        context = contextMerger.mergeForAccounting(context, route, fraudDecision);

        // ── EXIT POINT 4: Accounting ──────────────────────────────────
        PaymentContext accountingResult = accountingActivity.postAccounting(context);
        context = contextMerger.mergeAccountingResult(context, accountingResult);

        if (!context.isAccountingPosted()) {
            context.setCurrentStatus(PaymentStatus.FAILED);
            return buildResult(context);
        }

        // ── DECISION POINT: conditional activity based on route ────────
        if (route == RouteType.CROSS_BORDER) {
            PaymentContext complianceResult = complianceReportingActivity.fileCrossBorderReport(context);
            context = contextMerger.mergeComplianceResult(context, complianceResult);
        }

        // ── EXIT POINT 5: Payment Completion & Notifications ───────────
        PaymentContext completionResult = completionActivity.completeAndNotify(context);
        context = contextMerger.mergeCompletionResult(context, completionResult);

        return buildResult(context);
    }

    /**
     * Signal handler — an external system (e.g. a compliance review UI)
     * calls this to unblock a workflow parked in MANUAL_REVIEW.
     * Declare this as @SignalMethod on the PaymentWorkflow interface.
     */
    @Override
    public void submitReviewDecision(boolean approved) {
        context.setReviewDecisionReceived(true);
        context.setReviewApproved(approved);
    }

    @Override
    public PaymentContext getCurrentStatus() {
        return context;
    }

    // ─────────────────────────────────────────────────────────────────────
    // PRIVATE HELPERS
    // ─────────────────────────────────────────────────────────────────────

    /**
     * Builds the final PaymentResult from the accumulated context.
     */
    private PaymentResult buildResult(PaymentContext ctx) {
        return PaymentResult.builder()
            .paymentId(ctx.getAssignedPaymentId())
            .status(ctx.getCurrentStatus())
            .message(ctx.getFailureReason() != null ? ctx.getFailureReason()
                : ctx.getCompletionMessage() != null ? ctx.getCompletionMessage()
                : "Payment processed with status: " + ctx.getCurrentStatus())
            .amount(ctx.getOriginalRequest().getAmount())
            .currency(ctx.getOriginalRequest().getCurrency())
            .sourceAccountId(ctx.getOriginalRequest().getSourceAccountId())
            .destinationAccountId(ctx.getOriginalRequest().getDestinationAccountId())
            .auditTrail(ctx.getAuditTrail())
            .completedAt(LocalDateTime.now())
            .transactionReference(ctx.getTransactionReference())
            .build();
    }

    /**
     * Builds standardized ActivityOptions with retry configuration.
     *
     * @param scheduleToClose Maximum total time including all retries
     * @param maxAttempts     Maximum number of attempts (1 = no retry)
     * @param initialInterval Initial backoff interval between retries
     */
    private ActivityOptions buildActivityOptions(Duration scheduleToClose,
                                                  int maxAttempts,
                                                  Duration initialInterval) {
        return ActivityOptions.newBuilder()
            .setScheduleToCloseTimeout(scheduleToClose)
            .setRetryOptions(RetryOptions.newBuilder()
                .setMaximumAttempts(maxAttempts)
                .setInitialInterval(initialInterval)
                .setBackoffCoefficient(2.0)  // Exponential backoff multiplier
                .setMaximumInterval(Duration.ofSeconds(30))
                .build())
            .build();
    }
}
