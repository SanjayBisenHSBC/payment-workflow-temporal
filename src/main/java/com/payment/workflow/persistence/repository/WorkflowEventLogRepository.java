package com.payment.workflow.persistence.repository;

import com.payment.workflow.persistence.entity.WorkflowEventLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Repository for the immutable workflow audit event log.
 * Only findBy and save operations — no updates or deletes by design.
 */
@Repository
public interface WorkflowEventLogRepository extends JpaRepository<WorkflowEventLog, Long> {

    List<WorkflowEventLog> findByPaymentIdOrderByExecutedAtAsc(String paymentId);

    List<WorkflowEventLog> findByPaymentIdAndStepName(String paymentId, String stepName);

    long countByPaymentId(String paymentId);
}
