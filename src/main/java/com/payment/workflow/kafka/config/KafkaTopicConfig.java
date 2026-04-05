package com.payment.workflow.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * KafkaTopicConfig — declares all Kafka topics this service owns.
 *
 * Spring's KafkaAdmin (auto-configured by spring-kafka) will create these
 * topics on startup if they don't exist. In a production OpenShift environment
 * topics are typically pre-provisioned by the Kafka cluster admin (Strimzi
 * operator or AMQ Streams), so creation failures are non-fatal here.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * TOPIC DESIGN
 * ─────────────────────────────────────────────────────────────────────────────
 * payment.fraud.request    — 3 partitions, RF=2
 *   Producer: FraudCheckActivityImpl (this service)
 *   Consumer: External fraud screening system
 *   Key: correlationId (ensures request/response land on same partition)
 *
 * payment.fraud.response   — 3 partitions, RF=2
 *   Producer: External fraud screening system
 *   Consumer: FraudResponseConsumer (this service)
 *   Key: correlationId
 *
 * payment.events           — 6 partitions, RF=2
 *   Producer: PaymentEventProducer (this service, published at each step)
 *   Consumer: Notification service, reporting pipeline, ERP, audit
 *   Key: paymentId (guarantees ordering for a single payment)
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topics.fraud-request}")
    private String fraudRequestTopic;

    @Value("${app.kafka.topics.fraud-response}")
    private String fraudResponseTopic;

    @Value("${app.kafka.topics.payment-events}")
    private String paymentEventsTopic;

    @Bean
    public NewTopic fraudRequestTopic() {
        return TopicBuilder.name(fraudRequestTopic)
            .partitions(3)
            .replicas(1)   // Set to 2+ for production clusters
            .build();
    }

    @Bean
    public NewTopic fraudResponseTopic() {
        return TopicBuilder.name(fraudResponseTopic)
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic paymentEventsTopic() {
        return TopicBuilder.name(paymentEventsTopic)
            .partitions(6)
            .replicas(1)
            .build();
    }
}
