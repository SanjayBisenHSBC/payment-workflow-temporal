package com.payment.workflow.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Represents a single step in the payment workflow execution.
 * Used to build a complete audit trail for each payment.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowStep {

    /** Name of the step (e.g., INITIATION, VALIDATION) */
    private String stepName;

    /** Human-readable description of what happened */
    private String description;

    /** Whether this step completed successfully */
    private boolean success;

    /** Timestamp when this step was executed */
    private LocalDateTime executedAt;

    /** Any output data or derived values from this step */
    private String outputData;
}
