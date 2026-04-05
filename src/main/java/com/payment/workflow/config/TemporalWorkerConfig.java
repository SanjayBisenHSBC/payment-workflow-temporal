package com.payment.workflow.config;

import com.payment.workflow.activities.*;
import com.payment.workflow.workflow.PaymentWorkflowImpl;
import io.temporal.client.WorkflowClient;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

/**
 * TemporalWorkerConfig — Explicitly registers and starts the Temporal Worker.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * WHAT THIS CLASS DOES
 * ─────────────────────────────────────────────────────────────────────────────
 * 1. Creates a WorkerFactory from the WorkflowClient.
 * 2. Creates a Worker bound to "payment-task-queue".
 * 3. Registers PaymentWorkflowImpl (the orchestrator).
 * 4. Registers all 5 Activity implementations (the steps).
 * 5. Calls workerFactory.start() — this is the line that begins polling
 *    Temporal Server for tasks. Without this call, nothing runs.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * WHY @EventListener(ContextRefreshedEvent)?
 * ─────────────────────────────────────────────────────────────────────────────
 * We start the worker AFTER the full Spring context is initialised.
 * This guarantees all activity @Component beans are fully constructed and
 * injected before we hand them to the Temporal worker.
 *
 * Using @PostConstruct on a @Configuration class or a SmartLifecycle bean
 * can fire before all dependent beans are ready — @EventListener is safer.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * TASK QUEUE
 * ─────────────────────────────────────────────────────────────────────────────
 * The task queue name here MUST match the name used in:
 *   PaymentController → WorkflowOptions.setTaskQueue(TASK_QUEUE)
 *
 * If they don't match, workflows will be scheduled but never executed —
 * they'll sit in Temporal Server with status "Running" indefinitely.
 */
@Slf4j
@Configuration
public class TemporalWorkerConfig {

    private static final String TASK_QUEUE = "payment-task-queue";

    private final WorkflowClient workflowClient;

    // Activity implementations injected by Spring
    private final PaymentInitiationActivity  paymentInitiationActivity;
    private final PaymentValidationActivity  paymentValidationActivity;
    private final FraudCheckActivity         fraudCheckActivity;
    private final AccountingActivity         accountingActivity;
    private final PaymentCompletionActivity  paymentCompletionActivity;

    public TemporalWorkerConfig(
        WorkflowClient workflowClient,
        PaymentInitiationActivity paymentInitiationActivity,
        PaymentValidationActivity paymentValidationActivity,
        FraudCheckActivity fraudCheckActivity,
        AccountingActivity accountingActivity,
        PaymentCompletionActivity paymentCompletionActivity
    ) {
        this.workflowClient             = workflowClient;
        this.paymentInitiationActivity  = paymentInitiationActivity;
        this.paymentValidationActivity  = paymentValidationActivity;
        this.fraudCheckActivity         = fraudCheckActivity;
        this.accountingActivity         = accountingActivity;
        this.paymentCompletionActivity  = paymentCompletionActivity;
    }

    /**
     * Starts the Temporal Worker after the full Spring context is ready.
     *
     * Log line to confirm it worked:
     *   [Temporal] Worker started on task queue: payment-task-queue
     */
    @EventListener(ContextRefreshedEvent.class)
    public void startWorker() {
        log.info("[Temporal] Initialising WorkerFactory...");

        WorkerFactory factory = WorkerFactory.newInstance(workflowClient);

        // Create a worker that polls "payment-task-queue"
        Worker worker = factory.newWorker(TASK_QUEUE);

        // ── Register the Workflow implementation ──────────────────────────
        // Temporal uses this to reconstruct workflow state on replay.
        // Register the *Impl* class (not the interface).
        worker.registerWorkflowImplementationTypes(PaymentWorkflowImpl.class);
        log.info("[Temporal] Registered workflow: PaymentWorkflowImpl");

        // ── Register all Activity implementations ─────────────────────────
        // Pass the Spring-managed instances so they can use @Autowired deps.
        worker.registerActivitiesImplementations(
            paymentInitiationActivity,
            paymentValidationActivity,
            fraudCheckActivity,
            accountingActivity,
            paymentCompletionActivity
        );
        log.info("[Temporal] Registered 5 activities: Initiation, Validation, FraudCheck, Accounting, Completion");

        // ── Start polling ─────────────────────────────────────────────────
        // This is the critical call — without it nothing gets executed.
        // The factory starts background threads that long-poll Temporal Server.
        factory.start();

        log.info("[Temporal] ✓ Worker started on task queue: {}", TASK_QUEUE);
        log.info("[Temporal] ✓ Ready to process payments. Polling Temporal Server at: payment-task-queue");
    }
}
