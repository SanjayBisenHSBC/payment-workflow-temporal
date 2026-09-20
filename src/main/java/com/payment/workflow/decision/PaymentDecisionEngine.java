package com.payment.workflow.decision;

import com.payment.workflow.model.PaymentContext;

/**
 * Pure DECISION logic only. This class answers "what should happen
 * next?" — it never reshapes or writes data into PaymentContext.
 * That responsibility lives in PaymentContextMerger.
 *
 * Must stay side-effect-free (no I/O, no clock, no randomness) so it's
 * replay-safe to call directly from workflow code, and independently
 * unit-testable with plain JUnit.
 */
public class PaymentDecisionEngine {

    /**
     * Classifies the payment's route based on data already derived
     * during validation (currency, destination bank identifiers, etc).
     */
    public RouteType classifyRoute(PaymentContext ctx) {
        // TODO: replace with real logic — e.g. compare account/bank
        // country codes derived during validation.
        boolean crossBorder = !sameCountry(ctx);
        return crossBorder ? RouteType.CROSS_BORDER : RouteType.DOMESTIC;
    }

    /**
     * Decides the fraud outcome by combining the fraud score with the
     * route classification, instead of a flat boolean pass/fail. Reads
     * data only — never mutates ctx.
     */
    public FraudDecision evaluateFraudRisk(PaymentContext ctx, RouteType route) {
        if (!ctx.isFraudCheckPassed()) {
            return FraudDecision.BLOCK;
        }

        int riskScore = ctx.getRiskScore(); // assumes this field exists post fraud-check

        if (riskScore >= 80) {
            return FraudDecision.BLOCK;
        }
        if (riskScore >= 40) {
            return route == RouteType.CROSS_BORDER
                ? FraudDecision.MANUAL_REVIEW
                : FraudDecision.PROCEED;
        }
        return FraudDecision.PROCEED;
    }

    private boolean sameCountry(PaymentContext ctx) {
        // TODO: real country-code comparison
        return true;
    }
}
