package com.payment.workflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main entry point for the Payment Workflow Service.
 *
 * This application demonstrates a complete payment processing pipeline
 * using Temporal for durable workflow orchestration and Spring Boot
 * for the REST API layer.
 *
 * Architecture Overview:
 * ┌──────────────────────────────────────────────────────────────┐
 * │  REST Controller  →  Temporal Client  →  Temporal Server     │
 * │                                              ↓               │
 * │  PaymentWorkflow (Orchestrator)                              │
 * │       ↓                                                      │
 * │  Activities (Steps):                                         │
 * │    1. PaymentInitiation                                      │
 * │    2. PaymentValidation & Derivations                        │
 * │    3. FraudCheck                                             │
 * │    4. Accounting                                             │
 * │    5. PaymentCompletion & Notifications                      │
 * └──────────────────────────────────────────────────────────────┘
 */
@SpringBootApplication
public class PaymentWorkflowApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentWorkflowApplication.class, args);
    }
}
