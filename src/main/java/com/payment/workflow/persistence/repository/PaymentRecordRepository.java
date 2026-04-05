package com.payment.workflow.persistence.repository;

import com.payment.workflow.persistence.entity.PaymentRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Spring Data JPA repository for PaymentRecord.
 *
 * Spring auto-generates implementations for all standard CRUD methods.
 * Custom JPQL queries are added below for workflow-specific lookups.
 */
@Repository
public interface PaymentRecordRepository extends JpaRepository<PaymentRecord, Long> {

    Optional<PaymentRecord> findByPaymentId(String paymentId);

    Optional<PaymentRecord> findByWorkflowId(String workflowId);

    List<PaymentRecord> findBySourceAccountIdOrderByInitiatedAtDesc(String sourceAccountId);

    List<PaymentRecord> findByStatusOrderByInitiatedAtDesc(String status);

    boolean existsByPaymentId(String paymentId);

    /**
     * Partial update — only change the status column.
     * Used by activities to update status without loading the full entity.
     */
    @Modifying
    @Query("UPDATE PaymentRecord p SET p.status = :status WHERE p.paymentId = :paymentId")
    int updateStatus(@Param("paymentId") String paymentId, @Param("status") String status);
}
