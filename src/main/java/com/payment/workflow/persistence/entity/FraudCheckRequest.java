package com.payment.workflow.persistence.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;

/**
 * JPA Entity — maps to `fraud_check_requests`.
 *
 * Tracks the round-trip of every Kafka fraud check message:
 *   1. When we sent the request to the fraud topic
 *   2. When we received the response from the fraud topic
 *   3. The final risk score and decision
 *
 * This enables:
 *  - Debugging slow or missing fraud responses
 *  - SLA monitoring for fraud system latency
 *  - Replaying timed-out requests
 */
@Entity
@Table(name = "fraud_check_requests")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FraudCheckRequest {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "payment_id", nullable = false, unique = true, length = 64)
    private String paymentId;

    /** UUID used as Kafka message key for correlation (request → response matching) */
    @Column(name = "correlation_id", nullable = false, unique = true, length = 64)
    private String correlationId;

    @Column(name = "request_payload", nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String requestPayload;

    @Column(name = "response_payload", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String responsePayload;

    @Column(name = "request_sent_at", nullable = false)
    private OffsetDateTime requestSentAt;

    @Column(name = "response_received_at")
    private OffsetDateTime responseReceivedAt;

    /** PENDING → RESPONDED (success) or TIMEOUT */
    @Column(nullable = false, length = 20)
    private String status;

    @Column(name = "risk_score", length = 10)
    private String riskScore;

    @Column(name = "risk_details", columnDefinition = "TEXT")
    private String riskDetails;

    @PrePersist
    protected void onCreate() {
        if (requestSentAt == null) requestSentAt = OffsetDateTime.now();
        if (status == null) status = "PENDING";
    }
}
