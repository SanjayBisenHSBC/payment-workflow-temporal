package com.payment.workflow.config;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * TemporalConfig — Wires up the gRPC transport and high-level WorkflowClient.
 *
 * WHY NOT temporal-spring-boot-starter-alpha?
 * ─────────────────────────────────────────────────────────────────────────────
 * The alpha starter's auto-configuration reads `temporal.workers[*]` property
 * keys and tries to register workers automatically. In practice this is fragile:
 *
 *  - The exact property structure changed between minor SDK versions.
 *  - It conflicts silently with @Bean-defined WorkflowClient / WorkerFactory
 *    beans (both try to create the same beans, Spring gets confused).
 *  - Activity beans must be registered by bean name in YAML, which breaks if
 *    the bean name annotation doesn't match exactly.
 *  - There is no startup log confirming the worker actually started.
 *
 * SOLUTION — Explicit configuration (this file + TemporalWorkerConfig.java):
 *  - Full control over connection, namespace, and worker lifecycle.
 *  - Worker start is explicit and logged so you can confirm it worked.
 *  - Zero surprises from auto-configuration magic.
 *
 * Beans defined here:
 *   WorkflowServiceStubs  — raw gRPC channel to Temporal Server
 *   WorkflowClient        — high-level API used by REST controllers
 *
 * Worker registration lives in TemporalWorkerConfig.java (separate concern).
 */
@Slf4j
@Configuration
public class TemporalConfig {

    @Value("${app.temporal.target:127.0.0.1:7233}")
    private String temporalTarget;

    @Value("${app.temporal.namespace:default}")
    private String namespace;

    /**
     * Opens the gRPC channel to Temporal Server.
     * All SDK communication goes through this stub.
     */
    @Bean
    public WorkflowServiceStubs workflowServiceStubs() {
        log.info("[Temporal] Connecting to Temporal Server at: {}", temporalTarget);
        WorkflowServiceStubs stubs = WorkflowServiceStubs.newServiceStubs(
            WorkflowServiceStubsOptions.newBuilder()
                .setTarget(temporalTarget)
                .build()
        );
        log.info("[Temporal] gRPC channel established → {}", temporalTarget);
        return stubs;
    }

    /**
     * WorkflowClient — used by REST controllers to:
     *   - Start new workflow executions
     *   - Send signals to running workflows
     *   - Query live workflow state
     *   - Wait for workflow results
     */
    @Bean
    public WorkflowClient workflowClient(WorkflowServiceStubs stubs) {
        return WorkflowClient.newInstance(stubs,
            WorkflowClientOptions.newBuilder()
                .setNamespace(namespace)
                .build()
        );
    }
}
