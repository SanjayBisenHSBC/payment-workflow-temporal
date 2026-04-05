package com.payment.workflow.activities;

import com.payment.workflow.model.PaymentContext;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Activity Interface: Exit Point 5 — Payment Completion & Notifications
 *
 * ─────────────────────────────────────────────────────────────────
 * PURPOSE
 * ─────────────────────────────────────────────────────────────────
 * Finalizes the payment record and sends notifications to all parties.
 * This is the last step — the "close-out" of the workflow.
 *
 * ─────────────────────────────────────────────────────────────────
 * RESPONSIBILITIES
 * ─────────────────────────────────────────────────────────────────
 * 1. Generate final transaction reference number
 * 2. Update payment record status to COMPLETED
 * 3. Send confirmation notification to sender
 *    - Channel: Email, SMS, Push, Webhook
 *    - Content: Amount, recipient, reference, timestamp
 * 4. Send credit notification to receiver
 *    - Channel: Email, SMS, Push, Webhook
 *    - Content: Amount received, sender reference
 * 5. Trigger any downstream integrations (e.g., ERP, reporting)
 *
 * ─────────────────────────────────────────────────────────────────
 * NOTIFICATION FAILURE HANDLING
 * ─────────────────────────────────────────────────────────────────
 * Notification failures should NOT fail the payment itself — the funds
 * have already moved (accounting step). Handle notification failures
 * separately (e.g., retry via a different channel or queue for later).
 *
 * In this implementation, notification failures are logged but do not
 * cause the workflow to fail or be retried.
 */
@ActivityInterface
public interface PaymentCompletionActivity {

    /**
     * Completes the payment and sends all notifications.
     *
     * @param context Context with fully processed payment data
     * @return Context with final transaction reference and notification status
     */
    @ActivityMethod
    PaymentContext completeAndNotify(PaymentContext context);
}
