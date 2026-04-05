package com.payment.workflow.activities;

import com.payment.workflow.model.PaymentContext;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Activity Interface: Exit Point 3 — Fraud Check
 *
 * ─────────────────────────────────────────────────────────────────
 * PURPOSE
 * ─────────────────────────────────────────────────────────────────
 * Screens the payment for fraud, money laundering, and sanctions
 * compliance before any funds movement occurs.
 *
 * ─────────────────────────────────────────────────────────────────
 * CHECKS PERFORMED
 * ─────────────────────────────────────────────────────────────────
 * - Velocity check (too many payments in short time window)
 * - Amount threshold check (unusually large amounts)
 * - Sanctions screening (OFAC, EU, UN lists)
 * - Pattern analysis (unusual destination or frequency)
 * - AML (Anti Money Laundering) risk scoring
 *
 * ─────────────────────────────────────────────────────────────────
 * RISK SCORES
 * ─────────────────────────────────────────────────────────────────
 * LOW    → Payment passes, proceed normally
 * MEDIUM → Payment passes but flagged for manual review
 * HIGH   → Payment BLOCKED, workflow exits with FRAUD_BLOCKED status
 *
 * ─────────────────────────────────────────────────────────────────
 * REAL-WORLD INTEGRATION
 * ─────────────────────────────────────────────────────────────────
 * In production this would call an external fraud engine such as:
 * - NICE Actimize
 * - Oracle Financial Services AML
 * - Featurespace ARIC
 * - An in-house ML model endpoint
 */
@ActivityInterface
public interface FraudCheckActivity {

    /**
     * Performs fraud and AML screening on the payment.
     *
     * @param context Context with validated and enriched payment data
     * @return Context with fraud check results and risk score
     */
    @ActivityMethod
    PaymentContext performFraudCheck(PaymentContext context);
}
