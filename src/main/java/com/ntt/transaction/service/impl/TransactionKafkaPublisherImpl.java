package com.ntt.transaction.service.impl;

import com.ntt.transaction.config.TransactionKafkaProperties;
import com.ntt.transaction.model.entity.TransactionSaga;
import com.ntt.transaction.model.kafka.ApplyPaymentCommand;
import com.ntt.transaction.model.kafka.TransactionExecutedEvent;
import com.ntt.transaction.service.TransactionKafkaPublisher;
import io.reactivex.rxjava3.core.Completable;
import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.adapter.rxjava.RxJava3Adapter;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionKafkaPublisherImpl implements TransactionKafkaPublisher {

  private final ReactiveKafkaProducerTemplate<String, Object> producerTemplate;
  private final TransactionKafkaProperties kafkaProperties;

  @Override
  public Completable publishApplyPaymentCommand(TransactionSaga transactionSaga) {
    ApplyPaymentCommand command =
        ApplyPaymentCommand.builder()
            .creditId(transactionSaga.getTargetId())
            .amount(transactionSaga.getAmount())
            .sourceId(transactionSaga.getSourceId())
            .externalReference(transactionSaga.getId())
            .build();

    return RxJava3Adapter.monoToCompletable(
        producerTemplate
            .send(kafkaProperties.getApplyPaymentCommand(), transactionSaga.getId(), command)
            .doOnSuccess(
                result ->
                    log.info(
                        "ApplyPaymentCommand publicado para transaccion {}",
                        transactionSaga.getId()))
            .then());
  }

  @Override
  public Completable publishTransactionExecuted(TransactionSaga transactionSaga) {
    TransactionExecutedEvent event =
        TransactionExecutedEvent.builder()
            .transactionId(transactionSaga.getId())
            .type(transactionSaga.getType().name())
            .sourceId(transactionSaga.getSourceId())
            .targetId(transactionSaga.getTargetId())
            .amount(transactionSaga.getAmount())
            .status(transactionSaga.getStatus().name())
            .executedAt(
                transactionSaga.getCompletedAt() != null
                    ? transactionSaga.getCompletedAt()
                    : LocalDateTime.now())
            .build();

    return RxJava3Adapter.monoToCompletable(
        producerTemplate
            .send(kafkaProperties.getTransactionsExecuted(), transactionSaga.getId(), event)
            .doOnSuccess(
                result ->
                    log.info(
                        "TransactionExecutedEvent publicado para transaccion {}",
                        transactionSaga.getId()))
            .then());
  }
}
