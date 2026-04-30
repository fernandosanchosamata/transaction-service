package com.ntt.transaction.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ntt.transaction.config.TransactionKafkaProperties;
import com.ntt.transaction.model.entity.TransactionSaga;
import com.ntt.transaction.model.enums.SagaStatus;
import com.ntt.transaction.model.enums.TransactionType;
import com.ntt.transaction.model.kafka.ApplyPaymentCommand;
import com.ntt.transaction.model.kafka.TransactionExecutedEvent;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderResult;

@ExtendWith(MockitoExtension.class)
class TransactionKafkaPublisherImplTest {

  @Mock private ReactiveKafkaProducerTemplate<String, Object> producerTemplate;
  @Mock private SenderResult<Void> senderResult;

  private TransactionKafkaPublisherImpl publisher;
  private TransactionKafkaProperties properties;

  @BeforeEach
  void setUp() {
    properties = new TransactionKafkaProperties();
    publisher = new TransactionKafkaPublisherImpl(producerTemplate, properties);
  }

  @Test
  void publishApplyPaymentCommandSendsCommandToConfiguredTopic() {
    TransactionSaga saga = saga(TransactionType.PAYMENT);
    when(producerTemplate.send(
            eq(properties.getApplyPaymentCommand()), eq("tx-1"), any(ApplyPaymentCommand.class)))
        .thenReturn(Mono.just(senderResult));

    publisher.publishApplyPaymentCommand(saga).blockingAwait();

    verify(producerTemplate)
        .send(eq(properties.getApplyPaymentCommand()), eq("tx-1"), any(ApplyPaymentCommand.class));
  }

  @Test
  void publishTransactionExecutedSendsEventToConfiguredTopic() {
    TransactionSaga saga = saga(TransactionType.TRANSFER);
    saga.setCompletedAt(LocalDateTime.of(2026, 4, 29, 12, 0));
    when(producerTemplate.send(
            eq(properties.getTransactionsExecuted()),
            eq("tx-1"),
            any(TransactionExecutedEvent.class)))
        .thenReturn(Mono.just(senderResult));

    publisher.publishTransactionExecuted(saga).blockingAwait();

    verify(producerTemplate)
        .send(
            eq(properties.getTransactionsExecuted()),
            eq("tx-1"),
            any(TransactionExecutedEvent.class));
  }

  private TransactionSaga saga(TransactionType type) {
    return TransactionSaga.builder()
        .id("tx-1")
        .type(type)
        .sourceId("account-1")
        .targetId("credit-1")
        .amount(BigDecimal.valueOf(20))
        .status(SagaStatus.COMPLETED)
        .build();
  }
}
