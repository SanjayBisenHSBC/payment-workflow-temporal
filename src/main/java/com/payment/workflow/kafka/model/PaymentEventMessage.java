package com.payment.workflow.kafka.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Kafka message published to payment.events for downstream consumers.
 *
 * Downstream systems (notification service, reporting, ERP, audit) can
 * subscribe to this topic to react to payment lifecycle events without
 * coupling to the payment service's internal domain model.
 *
 * Event types:
 *  PAYMENT_INITIATED    — step 1 complete
 *  PAYMENT_VALIDATED    — step 2 complete
 *  FRAUD_CLEARED        — step 3 passed
 *  FRAUD_BLOCKED        — step 3 blocked the payment
 *  ACCOUNTING_POSTED    — step 4 complete
 *  PAYMENT_COMPLETED    — step 5 complete (final happy-path event)
 *  PAYMENT_FAILED       — any step failed
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEventMessage {

    private String eventId;
    private String eventType;
    private String paymentId;
    private String transactionReference;
    private String sourceAccountId;
    private String destinationAccountId;
    private BigDecimal amount;
    private String currency;
    private String status;
    private String details;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime eventTimestamp;
}
