package com.payment.workflow.decision;

import com.payment.workflow.model.PaymentContext;

/**
 * Sole responsibility: after EVERY activity call ("exit point"), take
 * the response and the running context, reconcile them, and return the
 * updated context. This is the one place in the workflow where "the
 * response we just got" becomes "the state we carry forward."
 *
 * No decisions are made here — no if/switch on business rules. If you
 * find yourself branching inside a merge*() method below, that logic
 * belongs in PaymentDecisionEngine instead, with the *result* of that
 * decision passed in as a parameter (see mergeForAccounting for the
 * pattern: it takes RouteType/FraudDecision as inputs, it doesn't
 * compute them).
 *
 * Must stay side-effect-free (no I/O, no clock, no randomness) for the
 * same replay-safety reason as the decision engine.
 */
public class PaymentContextMerger {

    /**
     * Called right after PaymentInitiationActivity responds.
     * NOTE: if/when initiationActivity is changed to return a narrower
     * result DTO instead of a full PaymentContext, this is the method
     * to update — copy the relevant fields (assignedPaymentId, initial
     * timestamps, audit entries) onto `current` instead of returning
     * `result` wholesale.
     */
    public PaymentContext mergeInitiationResult(PaymentContext current, PaymentContext result) {
        return result;
    }

    /** Called right after PaymentValidationActivity responds. */
    public PaymentContext mergeValidationResult(PaymentContext current, PaymentContext result) {
        return result;
    }

    /** Called right after FraudCheckActivity responds. */
    public PaymentContext mergeFraudCheckResult(PaymentContext current, PaymentContext result) {
        return result;
    }

    /**
     * Called right before AccountingActivity is invoked, to fold the
     * upstream decisions (route, fraud tier) into the fields accounting
     * actually needs — e.g. which settlement account to post against.
     * Takes decision *outputs* as plain parameters; does not compute them.
     */
    public PaymentContext mergeForAccounting(PaymentContext current, RouteType route, FraudDecision fraudDecision) {
        String settlementAccount = route == RouteType.CROSS_BORDER
            ? resolveCorrespondentAccount(current)
            : current.getOriginalRequest().getDestinationAccountId();

        current.setSettlementAccountId(settlementAccount); // add this field to PaymentContext
        return current;
    }

    /** Called right after AccountingActivity responds. */
    public PaymentContext mergeAccountingResult(PaymentContext current, PaymentContext result) {
        return result;
    }

    /** Called right after ComplianceReportingActivity responds (cross-border only). */
    public PaymentContext mergeComplianceResult(PaymentContext current, PaymentContext result) {
        return result;
    }

    /** Called right after PaymentCompletionActivity responds. */
    public PaymentContext mergeCompletionResult(PaymentContext current, PaymentContext result) {
        return result;
    }

    // ── private helpers — keep these pure too ──────────────────────────

    private String resolveCorrespondentAccount(PaymentContext ctx) {
        // TODO: real correspondent-bank resolution using data already
        // present in ctx from validation
        return ctx.getOriginalRequest().getDestinationAccountId();
    }
}
