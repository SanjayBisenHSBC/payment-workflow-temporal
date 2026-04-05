package com.payment.workflow.activities;

import com.payment.workflow.model.PaymentContext;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Activity Interface: Exit Point 4 — Accounting
 *
 * ─────────────────────────────────────────────────────────────────
 * PURPOSE
 * ─────────────────────────────────────────────────────────────────
 * Posts the double-entry bookkeeping entries for the payment.
 * This is the "funds movement" step — money is debited from the
 * source and credited to the destination.
 *
 * ─────────────────────────────────────────────────────────────────
 * ACCOUNTING ENTRIES
 * ─────────────────────────────────────────────────────────────────
 * For a payment of $100 from Account A to Account B:
 *
 * DEBIT  Account A (Source)      $100  (reduces balance)
 * CREDIT Account B (Destination) $100  (increases balance)
 *
 * For FX payments, an additional nostro/vostro account entry is made.
 *
 * ─────────────────────────────────────────────────────────────────
 * IDEMPOTENCY (CRITICAL)
 * ─────────────────────────────────────────────────────────────────
 * This activity MUST be idempotent. If it is retried (due to network
 * failure after DB write), it must NOT post duplicate entries.
 *
 * Strategy: Check if entries with the same paymentId already exist
 * before posting. If they do, return success without re-posting.
 *
 * ─────────────────────────────────────────────────────────────────
 * REAL-WORLD INTEGRATION
 * ─────────────────────────────────────────────────────────────────
 * In production this would call:
 * - A core banking system (Temenos, Finastra, Finacle)
 * - An in-house GL (General Ledger) service
 * - A message queue that drives the GL (e.g., Kafka → GL consumer)
 */
@ActivityInterface
public interface AccountingActivity {

    /**
     * Posts debit and credit ledger entries for the payment.
     *
     * @param context Context with fraud-cleared payment data
     * @return Context with accounting references and confirmation
     */
    @ActivityMethod
    PaymentContext postAccounting(PaymentContext context);
}
