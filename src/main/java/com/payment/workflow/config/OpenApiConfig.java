package com.payment.workflow.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI paymentWorkflowOpenAPI() {
        return new OpenAPI()
            .info(new Info()
                .title("Payment Workflow Service API")
                .description("""
                    ## Payment Processing Pipeline — Powered by Temporal
                    
                    This service exposes a REST API for submitting payments through a **5-step durable workflow**:
                    
                    | Step | Exit Point                        | Description                                      |
                    |------|-----------------------------------|--------------------------------------------------|
                    | 1    | Payment Initiation                | Assigns ID, booking reference, and timestamps    |
                    | 2    | Payment Validation & Derivations  | Validates rules; derives routing, FX, value date |
                    | 3    | Fraud Check                       | AML/fraud scoring; blocks HIGH risk payments     |
                    | 4    | Accounting                        | Posts debit/credit GL entries                    |
                    | 5    | Payment Completion & Notifications| Generates TXN ref; sends sender/receiver notices |
                    
                    ### Architecture
                    - **Framework**: Spring Boot 3.2 + Temporal SDK 1.22
                    - **Workflow Engine**: [Temporal](https://temporal.io) — durable, fault-tolerant orchestration
                    - **Task Queue**: `payment-task-queue`
                    - **Namespace**: `default`
                    
                    ### Key Design Principles
                    - All workflow steps are **idempotent** (safe to retry)
                    - Workflow state survives **process restarts**
                    - Each activity has **independent retry policies**
                    - Full **audit trail** is captured in every response
                    """)
                .version("1.0.0")
                .contact(new Contact()
                    .name("Payments Platform Team")
                    .email("payments@example.com"))
                .license(new License().name("Apache 2.0")))
            .servers(List.of(
                new Server().url("http://localhost:8080").description("Local Development")
            ));
    }
}
