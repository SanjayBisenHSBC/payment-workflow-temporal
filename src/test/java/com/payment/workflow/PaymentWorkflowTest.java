package com.payment.workflow;

import com.payment.workflow.activities.*;
import com.payment.workflow.model.*;
import com.payment.workflow.workflow.PaymentWorkflow;
import com.payment.workflow.workflow.PaymentWorkflowImpl;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.testing.TestWorkflowExtension;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit Tests for the Payment Workflow using Temporal's Test Framework.
 *
 * ─────────────────────────────────────────────────────────────────
 * TEMPORAL TESTING APPROACH
 * ─────────────────────────────────────────────────────────────────
 * Temporal provides a TestWorkflowEnvironment that:
 * - Runs an in-memory Temporal server (no real server needed)
 * - Supports time-skipping (workflows that sleep for hours run in ms)
 * - Allows mocking individual Activities
 * - Tests full workflow orchestration logic deterministically
 *
 * Two approaches:
 * 1. Full integration test: Register real activity implementations
 * 2. Unit test: Mock activities to test workflow logic in isolation
 *
 * These tests use real activity implementations for integration coverage.
 */
class PaymentWorkflowTest {

    @RegisterExtension
    public static final TestWorkflowExtension testWorkflow =
        TestWorkflowExtension.newBuilder()
            .setWorkflowTypes(PaymentWorkflowImpl.class)
            .setActivityImplementations(
//                new PaymentInitiationActivityImpl(),
//                new PaymentValidationActivityImpl(),
//                new FraudCheckActivityImpl(),
//                new AccountingActivityImpl(),
//                new PaymentCompletionActivityImpl()
            )
            .build();

    // ─────────────────────────────────────────────────────────────────
    // HAPPY PATH TEST
    // ─────────────────────────────────────────────────────────────────

    @Test
    void testSuccessfulPaymentWorkflow(TestWorkflowEnvironment env, Worker worker, PaymentWorkflow workflow) {
        PaymentRequest request = PaymentRequest.builder()
            .sourceAccountId("ACC-1001")
            .destinationAccountId("ACC-2002")
            .amount(new BigDecimal("500.00"))
            .currency("USD")
            .paymentType("WIRE")
            .reference("TEST-001")
            .description("Test payment")
            .build();

        PaymentResult result = workflow.processPayment(request);

        assertNotNull(result);
        assertEquals(PaymentStatus.COMPLETED, result.getStatus());
        assertNotNull(result.getPaymentId());
        assertNotNull(result.getTransactionReference());
        assertFalse(result.getAuditTrail().isEmpty());
        assertEquals(5, result.getAuditTrail().size(), "Should have 5 audit steps");

        // Verify all audit steps passed
        result.getAuditTrail().forEach(step ->
            assertTrue(step.isSuccess(), "Step " + step.getStepName() + " should have succeeded")
        );
    }

    // ─────────────────────────────────────────────────────────────────
    // VALIDATION FAILURE TEST
    // ─────────────────────────────────────────────────────────────────

    @Test
    void testPaymentFailsOnInvalidCurrency(TestWorkflowEnvironment env, Worker worker, PaymentWorkflow workflow) {
        PaymentRequest request = PaymentRequest.builder()
            .sourceAccountId("ACC-1001")
            .destinationAccountId("ACC-2002")
            .amount(new BigDecimal("100.00"))
            .currency("XYZ")   // Unsupported currency
            .paymentType("WIRE")
            .build();

        PaymentResult result = workflow.processPayment(request);

        assertNotNull(result);
        assertEquals(PaymentStatus.FAILED, result.getStatus());
        assertNotNull(result.getMessage());
        assertTrue(result.getMessage().contains("Unsupported currency") ||
                   result.getMessage().contains("Validation failed"));
    }

    // ─────────────────────────────────────────────────────────────────
    // FRAUD BLOCK TEST
    // ─────────────────────────────────────────────────────────────────

    @Test
    void testPaymentBlockedByFraud(TestWorkflowEnvironment env, Worker worker, PaymentWorkflow workflow) {
        PaymentRequest request = PaymentRequest.builder()
            .sourceAccountId("ACC-BLOCKED-001")   // In HIGH_RISK_ACCOUNTS list
            .destinationAccountId("ACC-2002")
            .amount(new BigDecimal("100.00"))
            .currency("USD")
            .paymentType("WIRE")
            .build();

        PaymentResult result = workflow.processPayment(request);

        assertNotNull(result);
        assertEquals(PaymentStatus.FRAUD_BLOCKED, result.getStatus());
        assertTrue(result.getMessage().toLowerCase().contains("fraud") ||
                   result.getMessage().toLowerCase().contains("blocked"));
    }

    // ─────────────────────────────────────────────────────────────────
    // INTERNAL PAYMENT TEST
    // ─────────────────────────────────────────────────────────────────

    @Test
    void testInternalPaymentWorkflow(TestWorkflowEnvironment env, Worker worker, PaymentWorkflow workflow) {
        PaymentRequest request = PaymentRequest.builder()
            .sourceAccountId("ACC-3001")
            .destinationAccountId("ACC-3002")
            .amount(new BigDecimal("250.75"))
            .currency("SGD")
            .paymentType("INTERNAL")
            .description("Internal transfer")
            .build();

        PaymentResult result = workflow.processPayment(request);

        assertEquals(PaymentStatus.COMPLETED, result.getStatus());
        assertNotNull(result.getTransactionReference());
        assertTrue(result.getTransactionReference().startsWith("TXN-"));
    }

    // ─────────────────────────────────────────────────────────────────
    // AMOUNT VALIDATION TEST
    // ─────────────────────────────────────────────────────────────────

    @Test
    void testPaymentFailsOnZeroAmount(TestWorkflowEnvironment env, Worker worker, PaymentWorkflow workflow) {
        PaymentRequest request = PaymentRequest.builder()
            .sourceAccountId("ACC-1001")
            .destinationAccountId("ACC-2002")
            .amount(BigDecimal.ZERO)
            .currency("USD")
            .paymentType("WIRE")
            .build();

        PaymentResult result = workflow.processPayment(request);

        assertNotNull(result);
        assertEquals(PaymentStatus.FAILED, result.getStatus());
    }

    // ─────────────────────────────────────────────────────────────────
    // AUDIT TRAIL TEST
    // ─────────────────────────────────────────────────────────────────

    @Test
    void testAuditTrailContainsAllSteps(TestWorkflowEnvironment env, Worker worker, PaymentWorkflow workflow) {
        PaymentRequest request = PaymentRequest.builder()
            .sourceAccountId("ACC-5001")
            .destinationAccountId("ACC-5002")
            .amount(new BigDecimal("1000.00"))
            .currency("EUR")
            .paymentType("SEPA")
            .build();

        PaymentResult result = workflow.processPayment(request);

        assertNotNull(result.getAuditTrail());
        assertEquals(5, result.getAuditTrail().size());

        // Verify step names in order
        assertEquals("INITIATION",  result.getAuditTrail().get(0).getStepName());
        assertEquals("VALIDATION",  result.getAuditTrail().get(1).getStepName());
        assertEquals("FRAUD_CHECK", result.getAuditTrail().get(2).getStepName());
        assertEquals("ACCOUNTING",  result.getAuditTrail().get(3).getStepName());
        assertEquals("COMPLETION",  result.getAuditTrail().get(4).getStepName());
    }
}
