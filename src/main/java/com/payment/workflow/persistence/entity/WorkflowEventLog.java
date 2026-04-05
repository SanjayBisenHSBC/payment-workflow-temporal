package com.payment.workflow.persistence.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;

/**
 * JPA Entity — maps to the `workflow_event_log` table.
 *
 * Immutable append-only audit log. One row is inserted for each
 * workflow step (exit point) as it completes or fails.
 *
 * This table is NEVER updated — only inserted. It provides:
 *  - A queryable history of every action on every payment
 *  - Evidence for regulatory audits (PCI DSS, MAS, SWIFT CSP)
 *  - Debugging data when a payment fails mid-flow
 *
 * The output_data column stores JSONB so step-specific structured
 * output (e.g., FX rate, accounting reference) can be queried in SQL.
 */
@Entity
@Table(name = "workflow_event_log")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowEventLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "payment_id", nullable = false, length = 64)
    private String paymentId;

    @Column(name = "workflow_id", nullable = false, length = 64)
    private String workflowId;

    /**
     * Which workflow exit point produced this event.
     * Values: INITIATION | VALIDATION | FRAUD_CHECK | ACCOUNTING | COMPLETION
     */
    @Column(name = "step_name", nullable = false, length = 64)
    private String stepName;

    /**
     * Fine-grained event type within the step.
     * Values: STEP_STARTED | STEP_COMPLETED | STEP_FAILED | STEP_BLOCKED
     */
    @Column(name = "event_type", nullable = false, length = 64)
    private String eventType;

    /** Outcome of this step: SUCCESS | FAILURE | BLOCKED */
    @Column(nullable = false, length = 32)
    private String status;

    @Column(columnDefinition = "TEXT")
    private String description;

    /**
     * Structured JSON output from the step — stored as JSONB in Postgres.
     * Example: {"fxRate":"0.74","route":"CROSS_BORDER","valueDate":"2024-03-17"}
     */
    @Column(name = "output_data", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String outputData;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "executed_at", nullable = false)
    private OffsetDateTime executedAt;

    @PrePersist
    protected void onCreate() {
        if (executedAt == null) executedAt = OffsetDateTime.now();
    }
}
