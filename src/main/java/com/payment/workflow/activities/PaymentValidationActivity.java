package com.payment.workflow.activities;

import com.payment.workflow.model.PaymentContext;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Activity Interface: Exit Point 2 — Payment Validation & Derivations
 *
 * ─────────────────────────────────────────────────────────────────
 * PURPOSE
 * ─────────────────────────────────────────────────────────────────
 * Validates business rules and derives processing parameters needed
 * for downstream steps. This is the "enrichment" step.
 *
 * ─────────────────────────────────────────────────────────────────
 * VALIDATION CHECKS
 * ─────────────────────────────────────────────────────────────────
 * - Amount > 0 and within limits
 * - Source and destination accounts exist and are active
 * - Sufficient balance in source account
 * - Currency is supported
 * - Payment type is valid for the route
 * - Source and destination are not same account
 *
 * ─────────────────────────────────────────────────────────────────
 * DERIVATIONS (Enrichment)
 * ─────────────────────────────────────────────────────────────────
 * - Payment routing (INTERNAL / DOMESTIC / CROSS_BORDER)
 * - Correspondent bank (for SWIFT/SEPA)
 * - FX rate (if currency conversion required)
 * - Converted amount in settlement currency
 * - Value date (T+0, T+1, T+2 based on payment type)
 * - Charge bearer (OUR / BEN / SHA)
 */
@ActivityInterface
public interface PaymentValidationActivity {

    /**
     * Validates the payment and derives all processing parameters.
     *
     * @param context Context with initiated payment data
     * @return Enriched context with validation results and derived fields
     */
    @ActivityMethod
    PaymentContext validateAndDerive(PaymentContext context);
}
