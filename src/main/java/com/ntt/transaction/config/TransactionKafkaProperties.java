package com.ntt.transaction.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "transaction.kafka.topics")
public class TransactionKafkaProperties {
  private String applyPaymentCommand = "apply-payment-command-topic";
  private String paymentApplied = "payment-applied-topic";
  private String paymentRejected = "payment-rejected-topic";
  private String transactionsExecuted = "transactions-executed-topic";
}
