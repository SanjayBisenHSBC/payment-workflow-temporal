package com.payment.workflow.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Represents a payment request submitted by a client.
 * This is the initial input that kicks off the entire workflow.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentRequest {

    /** Unique identifier for the payment (generated if not provided) */
    private String paymentId;

    /** The sender's account number */
    private String sourceAccountId;

    /** The recipient's account number */
    private String destinationAccountId;

    /** Payment amount */
    private BigDecimal amount;

    /** ISO 4217 currency code (e.g., USD, EUR, SGD) */
    private String currency;

    /** Payment type: WIRE, ACH, SEPA, SWIFT, INTERNAL */
    private String paymentType;

    /** Optional reference or description from the sender */
    private String reference;

    /** Customer-facing description */
    private String description;

    /** Sender's metadata (IP address, device info etc.) */
    private String senderMetadata;
}
