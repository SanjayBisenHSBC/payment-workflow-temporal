package com.payment.workflow.activities;

import com.payment.workflow.kafka.producer.PaymentEventProducer;
import com.payment.workflow.model.PaymentContext;
import com.payment.workflow.model.PaymentStatus;
import com.payment.workflow.model.WorkflowStep;
import com.payment.workflow.persistence.service.PaymentPersistenceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Exit Point 2 — Payment Validation & Derivations
 *
 * Changes from v1:
 *  + Persists validation results via PaymentPersistenceService
 *  + Publishes PAYMENT_VALIDATED or PAYMENT_FAILED event to Kafka
 */
@Slf4j
@Component("paymentValidationActivity")
@RequiredArgsConstructor
public class PaymentValidationActivityImpl implements PaymentValidationActivity {

    private final PaymentPersistenceService persistenceService;
    private final PaymentEventProducer eventProducer;

    private static final Set<String> SUPPORTED_CURRENCIES = Set.of(
        "USD", "EUR", "GBP", "SGD", "JPY", "CHF", "AUD", "CAD", "HKD", "CNY");
    private static final Set<String> SUPPORTED_PAYMENT_TYPES = Set.of(
        "WIRE", "ACH", "SEPA", "SWIFT", "INTERNAL");
    private static final BigDecimal MAX_AMOUNT = new BigDecimal("10000000.00");
    private static final BigDecimal MIN_AMOUNT = new BigDecimal("0.01");

    @Override
    public PaymentContext validateAndDerive(PaymentContext context) {
        log.info("[VALIDATION] Starting for paymentId={}", context.getAssignedPaymentId());
        List<String> errors = new ArrayList<>();
        var req = context.getOriginalRequest();

        try {
            // ── Validations ────────────────────────────────────────────────
            if (req.getAmount() == null || req.getAmount().compareTo(MIN_AMOUNT) < 0)
                errors.add("Amount must be >= " + MIN_AMOUNT);
            if (req.getAmount() != null && req.getAmount().compareTo(MAX_AMOUNT) > 0)
                errors.add("Amount exceeds max limit of " + MAX_AMOUNT);
            if (req.getCurrency() == null || !SUPPORTED_CURRENCIES.contains(req.getCurrency().toUpperCase()))
                errors.add("Unsupported currency: " + req.getCurrency());
            if (req.getSourceAccountId() == null || req.getSourceAccountId().isBlank())
                errors.add("Source account is required");
            if (req.getDestinationAccountId() == null || req.getDestinationAccountId().isBlank())
                errors.add("Destination account is required");
            if (req.getSourceAccountId() != null
                && req.getSourceAccountId().equals(req.getDestinationAccountId()))
                errors.add("Source and destination accounts cannot be the same");
            if (req.getPaymentType() == null
                || !SUPPORTED_PAYMENT_TYPES.contains(req.getPaymentType().toUpperCase()))
                errors.add("Unsupported payment type: " + req.getPaymentType());
            if (req.getAmount() != null && req.getAmount().compareTo(new BigDecimal("999999.99")) > 0)
                errors.add("Insufficient funds for amount: " + req.getAmount());

            if (!errors.isEmpty()) {
                String errorMsg = String.join("; ", errors);
                log.warn("[VALIDATION] ✗ Failed for {}: {}", context.getAssignedPaymentId(), errorMsg);
                context.setValidationPassed(false);
                context.setValidationErrors(errorMsg);
                context.setCurrentStatus(PaymentStatus.FAILED);
                context.setFailureReason("Validation failed: " + errorMsg);
                context.addAuditStep(WorkflowStep.builder()
                    .stepName("VALIDATION").description("Validation failed: " + errorMsg)
                    .success(false).executedAt(LocalDateTime.now()).build());

                persistenceService.saveValidation(context);
                eventProducer.publishFailed(context, errorMsg);
                return context;
            }

            // ── Derivations ────────────────────────────────────────────────
            String route = deriveRoute(req.getPaymentType());
            context.setPaymentRoute(route);
            if ("CROSS_BORDER".equals(route) || "SWIFT".equalsIgnoreCase(req.getPaymentType()))
                context.setCorrespondentBank(deriveCorrespondentBank(req.getCurrency()));

            BigDecimal fxRate = deriveFxRate(req.getCurrency());
            context.setFxRate(fxRate);
            context.setSettlementCurrency("SGD");
            context.setConvertedAmount("SGD".equalsIgnoreCase(req.getCurrency())
                ? req.getAmount()
                : req.getAmount().divide(fxRate, 2, RoundingMode.HALF_UP));
            context.setValueDate(deriveValueDate(req.getPaymentType()));
            context.setChargeBearer(deriveChargeBearer(req.getPaymentType()));
            context.setValidationPassed(true);
            context.setCurrentStatus(PaymentStatus.VALIDATED);

            String derivedSummary = String.format(
                "Route=%s, FX=%s, ValueDate=%s, ChargeBearer=%s",
                route, fxRate, context.getValueDate(), context.getChargeBearer());

            context.addAuditStep(WorkflowStep.builder()
                .stepName("VALIDATION").description("Validation passed. " + derivedSummary)
                .success(true).executedAt(LocalDateTime.now()).outputData(derivedSummary).build());

            // ── Persist + publish ──────────────────────────────────────────
            persistenceService.saveValidation(context);
            eventProducer.publishValidated(context);

            log.info("[VALIDATION] ✓ Complete for paymentId={}", context.getAssignedPaymentId());

        } catch (Exception e) {
            log.error("[VALIDATION] ✗ Unexpected error", e);
            context.setValidationPassed(false);
            context.setCurrentStatus(PaymentStatus.FAILED);
            context.setFailureReason("Validation error: " + e.getMessage());
            context.addAuditStep(WorkflowStep.builder()
                .stepName("VALIDATION").description("Error: " + e.getMessage())
                .success(false).executedAt(LocalDateTime.now()).build());
            persistenceService.saveValidation(context);
        }

        return context;
    }

    private String deriveRoute(String type) {
        return switch (type.toUpperCase()) {
            case "INTERNAL" -> "INTERNAL";
            case "ACH"      -> "DOMESTIC";
            case "SEPA"     -> "REGIONAL";
            default         -> "CROSS_BORDER";
        };
    }

    private String deriveCorrespondentBank(String currency) {
        return switch (currency.toUpperCase()) {
            case "USD" -> "CHASUS33 (JPMorgan Chase, New York)";
            case "EUR" -> "DEUTDEDB (Deutsche Bank, Frankfurt)";
            case "GBP" -> "BARCGB22 (Barclays, London)";
            case "JPY" -> "BOTKJPJT (MUFG, Tokyo)";
            default    -> "UOVBSGSG (UOB, Singapore)";
        };
    }

    private BigDecimal deriveFxRate(String currency) {
        return switch (currency.toUpperCase()) {
            case "USD" -> new BigDecimal("0.74");
            case "EUR" -> new BigDecimal("0.69");
            case "GBP" -> new BigDecimal("0.59");
            case "JPY" -> new BigDecimal("112.50");
            case "CHF" -> new BigDecimal("0.67");
            case "AUD" -> new BigDecimal("1.14");
            case "CAD" -> new BigDecimal("1.01");
            case "HKD" -> new BigDecimal("5.81");
            case "CNY" -> new BigDecimal("5.40");
            default    -> BigDecimal.ONE;
        };
    }

    private String deriveValueDate(String type) {
        LocalDate today = LocalDate.now();
        return switch (type.toUpperCase()) {
            case "INTERNAL" -> today.format(DateTimeFormatter.ISO_DATE);
            case "ACH", "SEPA" -> today.plusDays(1).format(DateTimeFormatter.ISO_DATE);
            default -> today.plusDays(2).format(DateTimeFormatter.ISO_DATE);
        };
    }

    private String deriveChargeBearer(String type) {
        return switch (type.toUpperCase()) {
            case "INTERNAL" -> "OUR";
            case "SEPA"     -> "SHA";
            default         -> "OUR";
        };
    }
}
