package com.payment.workflow.persistence.repository;

import com.payment.workflow.persistence.entity.FraudCheckRequest;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface FraudCheckRequestRepository extends JpaRepository<FraudCheckRequest, Long> {

    Optional<FraudCheckRequest> findByPaymentId(String paymentId);

    Optional<FraudCheckRequest> findByCorrelationId(String correlationId);
}
