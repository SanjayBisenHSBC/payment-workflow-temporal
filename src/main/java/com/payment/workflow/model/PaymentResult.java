package com.payment.workflow.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Final result of the payment workflow.
 * Contains the complete audit trail of each processing step.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentResult {

    private String paymentId;
    private PaymentStatus status;
    private String message;
    private BigDecimal amount;
    private String currency;
    private String sourceAccountId;
    private String destinationAccountId;

    /** Step-by-step audit trail of workflow execution */
    private List<WorkflowStep> auditTrail;

    /** Timestamp when the workflow completed */
    private LocalDateTime completedAt;

    /** Transaction reference number (assigned during processing) */
    private String transactionReference;
}
