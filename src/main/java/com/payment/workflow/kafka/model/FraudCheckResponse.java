package com.payment.workflow.kafka.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Kafka message received from the fraud system on topic: payment.fraud.response
 *
 * The correlationId matches the one sent in FraudCheckRequestMessage,
 * allowing the waiting thread (in FraudCheckActivityImpl) to match
 * this response to the original request.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FraudCheckResponse {

    /** Must match the correlationId from FraudCheckRequestMessage */
    private String correlationId;

    private String paymentId;

    /** LOW | MEDIUM | HIGH */
    private String riskLevel;

    /** Numeric score 0–100 */
    private int riskScore;

    /** Human-readable list of risk factors identified */
    private String riskDetails;

    /** Whether the fraud system recommends blocking this payment */
    private boolean blocked;

    /** Whether manual review is recommended */
    private boolean manualReviewRequired;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime responseTimestamp;
}
