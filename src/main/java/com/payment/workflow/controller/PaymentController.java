package com.payment.workflow.controller;

import com.payment.workflow.model.*;
import com.payment.workflow.workflow.PaymentWorkflow;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowNotFoundException;
import io.temporal.client.WorkflowOptions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/**
 * REST Controller — exposes payment workflow operations over HTTP.
 *
 * The WorkflowClient is injected from TemporalConfig (a Spring @Bean).
 * The task queue name must match the one registered in TemporalWorkerConfig.
 */
@Slf4j
@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
@Tag(name = "Payment Workflow", description = "Payment processing workflow API backed by Temporal")
public class PaymentController {

    private final WorkflowClient workflowClient;

    @Value("${app.temporal.task-queue:payment-task-queue}")
    private String taskQueue;

    // ─────────────────────────────────────────────────────────────────
    // POST /api/payments — synchronous (blocks until workflow finishes)
    // ─────────────────────────────────────────────────────────────────

    @PostMapping
    @Operation(
        summary = "Submit a payment (synchronous)",
        description = """
            Starts the full 5-step payment workflow and waits for completion.
            
            Steps executed:
            1. Initiation — assigns payment ID and booking reference
            2. Validation & Derivations — validates rules, derives FX/routing/value-date
            3. Fraud Check — AML scoring; HIGH risk = FRAUD_BLOCKED
            4. Accounting — posts debit + credit GL entries
            5. Completion & Notifications — generates TXN ref, sends notifications
            """,
        requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(
            content = @Content(
                mediaType = "application/json",
                examples = @ExampleObject(name = "Wire Payment", value = """
                    {
                      "sourceAccountId": "ACC-1001",
                      "destinationAccountId": "ACC-2002",
                      "amount": 1500.00,
                      "currency": "USD",
                      "paymentType": "WIRE",
                      "reference": "INV-2024-001",
                      "description": "Invoice payment"
                    }
                    """)
            )
        ),
        responses = {
            @ApiResponse(responseCode = "200", description = "Payment completed successfully"),
            @ApiResponse(responseCode = "402", description = "Payment failed or was blocked by fraud"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
        }
    )
    public ResponseEntity<PaymentResult> submitPayment(@RequestBody PaymentRequest request) {
        if (request.getPaymentId() == null || request.getPaymentId().isBlank()) {
            request.setPaymentId("PAY-" + UUID.randomUUID().toString().substring(0, 12).toUpperCase());
        }

        log.info("[Controller] Received payment: id={} amount={} {} type={}",
            request.getPaymentId(), request.getAmount(), request.getCurrency(), request.getPaymentType());

        WorkflowOptions options = WorkflowOptions.newBuilder()
            .setWorkflowId(request.getPaymentId())
            .setTaskQueue(taskQueue)
            .setWorkflowExecutionTimeout(Duration.ofMinutes(10))
            .setWorkflowRunTimeout(Duration.ofMinutes(5))
            .build();

        PaymentWorkflow workflow = workflowClient.newWorkflowStub(PaymentWorkflow.class, options);

        try {
            PaymentResult result = workflow.processPayment(request);
            log.info("[Controller] Payment {} → status={}", result.getPaymentId(), result.getStatus());

            HttpStatus status = switch (result.getStatus()) {
                case COMPLETED -> HttpStatus.OK;
                default -> HttpStatus.PAYMENT_REQUIRED;
            };
            return ResponseEntity.status(status).body(result);

        } catch (Exception e) {
            log.error("[Controller] Workflow failed for payment {}: {}", request.getPaymentId(), e.getMessage(), e);
            PaymentResult error = PaymentResult.builder()
                .paymentId(request.getPaymentId())
                .status(PaymentStatus.FAILED)
                .message("Workflow execution failed: " + e.getMessage())
                .amount(request.getAmount())
                .currency(request.getCurrency())
                .build();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // POST /api/payments/async — fire-and-forget, returns immediately
    // ─────────────────────────────────────────────────────────────────

    @PostMapping("/async")
    @Operation(
        summary = "Submit a payment (asynchronous)",
        description = "Starts the workflow and returns immediately. Poll /api/payments/{id}/status to track progress.",
        responses = {
            @ApiResponse(responseCode = "202", description = "Workflow started")
        }
    )
    public ResponseEntity<Map<String, String>> submitPaymentAsync(@RequestBody PaymentRequest request) {
        if (request.getPaymentId() == null || request.getPaymentId().isBlank()) {
            request.setPaymentId("PAY-" + UUID.randomUUID().toString().substring(0, 12).toUpperCase());
        }

        WorkflowOptions options = WorkflowOptions.newBuilder()
            .setWorkflowId(request.getPaymentId())
            .setTaskQueue(taskQueue)
            .setWorkflowExecutionTimeout(Duration.ofMinutes(10))
            .build();

        PaymentWorkflow workflow = workflowClient.newWorkflowStub(PaymentWorkflow.class, options);
        WorkflowClient.start(workflow::processPayment, request);

        log.info("[Controller] Async workflow started: {}", request.getPaymentId());

        return ResponseEntity.accepted().body(Map.of(
            "paymentId", request.getPaymentId(),
            "status", "PROCESSING",
            "statusUrl", "/api/payments/" + request.getPaymentId() + "/status"
        ));
    }

    // ─────────────────────────────────────────────────────────────────
    // GET /api/payments/{paymentId}/status — live query
    // ─────────────────────────────────────────────────────────────────

    @GetMapping("/{paymentId}/status")
    @Operation(
        summary = "Query live workflow status",
        description = "Reads the in-memory PaymentContext of a running or recently completed workflow.",
        responses = {
            @ApiResponse(responseCode = "200", description = "Current workflow state"),
            @ApiResponse(responseCode = "404", description = "Workflow not found")
        }
    )
    public ResponseEntity<?> getPaymentStatus(
        @Parameter(description = "Payment ID (also used as Temporal workflow ID)")
        @PathVariable String paymentId
    ) {
        try {
            PaymentWorkflow stub = workflowClient.newWorkflowStub(PaymentWorkflow.class, paymentId);
            PaymentContext context = stub.getCurrentStatus();
            return ResponseEntity.ok(context);
        } catch (WorkflowNotFoundException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "No workflow found for paymentId: " + paymentId));
        } catch (Exception e) {
            log.warn("[Controller] Status query failed for {}: {}", paymentId, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "Could not retrieve status: " + e.getMessage()));
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // GET /api/payments/health
    // ─────────────────────────────────────────────────────────────────

    @GetMapping("/health")
    @Operation(summary = "Service health check")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
            "service", "payment-workflow-service",
            "temporalTaskQueue", taskQueue,
            "status", "UP"
        ));
    }
}
