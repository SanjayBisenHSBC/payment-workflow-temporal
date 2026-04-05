package com.payment.workflow.kafka.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Kafka message sent to the fraud system on topic: payment.fraud.request
 *
 * The correlationId is used to match a response back to its original request.
 * It is set as the Kafka message key for partition affinity and as a payload
 * field for easy correlation on the consumer side.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FraudCheckRequestMessage {

    /** Unique ID for this request — used to correlate the response */
    private String correlationId;

    /** The payment being screened */
    private String paymentId;

    private String sourceAccountId;
    private String destinationAccountId;
    private BigDecimal amount;
    private String currency;
    private String paymentType;
    private String paymentRoute;
    private String senderMetadata;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime requestTimestamp;
}
