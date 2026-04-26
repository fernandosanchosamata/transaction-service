package com.ntt.transaction.service.impl;

import com.ntt.transaction.client.AccountClient;
import com.ntt.transaction.model.dto.PaymentRequest;
import com.ntt.transaction.model.dto.TransferRequest;
import com.ntt.transaction.model.entity.TransactionSaga;
import com.ntt.transaction.model.enums.SagaStatus;
import com.ntt.transaction.model.enums.TransactionType;
import com.ntt.transaction.model.kafka.PaymentAppliedEvent;
import com.ntt.transaction.model.kafka.PaymentRejectedEvent;
import com.ntt.transaction.repository.TransactionSagaRepository;
import com.ntt.transaction.service.TransactionKafkaPublisher;
import com.ntt.transaction.service.TransactionService;
import io.reactivex.rxjava3.core.Single;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionServiceImpl implements TransactionService {

  private final TransactionSagaRepository transactionRepository;
  private final AccountClient accountClient;
  private final TransactionKafkaPublisher transactionKafkaPublisher;

  @Override
  public Single<TransactionSaga> transfer(TransferRequest request) {
    validateDistinctEndpoints(
        request.getSourceAccountId(), request.getTargetAccountId(), "transferencia");

    return createSaga(
            TransactionType.TRANSFER,
            request.getSourceAccountId(),
            request.getTargetAccountId(),
            request.getAmount())
        .flatMap(saga -> transition(saga, SagaStatus.VALIDATING_FUNDS, null))
        .flatMap(
            validating ->
                accountClient
                    .withdraw(validating.getSourceId(), validating.getAmount())
                    .flatMap(ignored -> transition(validating, SagaStatus.FUNDS_RESERVED, null))
                    .flatMap(
                        reserved ->
                            accountClient
                                .deposit(reserved.getTargetId(), reserved.getAmount())
                                .flatMap(ignored -> completeSaga(reserved))
                                .onErrorResumeNext(
                                    error -> compensateSourceAccount(reserved, error)))
                    .onErrorResumeNext(error -> failSaga(validating, error)));
  }

  @Override
  public Single<TransactionSaga> pay(PaymentRequest request) {
    validateDistinctEndpoints(request.getSourceId(), request.getTargetId(), "pago");

    return createSaga(
            TransactionType.PAYMENT,
            request.getSourceId(),
            request.getTargetId(),
            request.getAmount())
        .flatMap(saga -> transition(saga, SagaStatus.VALIDATING_FUNDS, null))
        .flatMap(
            validating ->
                accountClient
                    .withdraw(validating.getSourceId(), validating.getAmount())
                    .flatMap(ignored -> transition(validating, SagaStatus.FUNDS_RESERVED, null))
                    .flatMap(
                        reserved ->
                            transactionKafkaPublisher
                                .publishApplyPaymentCommand(reserved)
                                .andThen(
                                    transition(reserved, SagaStatus.CREDITING_DESTINATION, null))
                                .onErrorResumeNext(
                                    error -> compensateSourceAccount(reserved, error)))
                    .onErrorResumeNext(error -> failSaga(validating, error)));
  }

  @Override
  public Single<TransactionSaga> getTransaction(String transactionId) {
    return transactionRepository
        .findById(transactionId)
        .switchIfEmpty(Single.error(new IllegalArgumentException("Transaccion no encontrada.")));
  }

  @Override
  public Single<TransactionSaga> handlePaymentApplied(PaymentAppliedEvent event) {
    if (event.getExternalReference() == null || event.getExternalReference().isBlank()) {
      return Single.error(
          new IllegalArgumentException("PaymentAppliedEvent recibido sin externalReference."));
    }

    return getTransaction(event.getExternalReference())
        .flatMap(
            transaction -> {
              if (transaction.getStatus() != SagaStatus.CREDITING_DESTINATION) {
                log.info(
                    "Ignorando PaymentAppliedEvent para transaccion {} en estado {}",
                    transaction.getId(),
                    transaction.getStatus());
                return Single.just(transaction);
              }

              transaction.setStatus(SagaStatus.COMPLETED);
              transaction.setErrorMessage(null);
              transaction.setUpdatedAt(resolveEventTime(event.getAppliedAt()));
              transaction.setCompletedAt(resolveEventTime(event.getAppliedAt()));

              return transactionRepository
                  .save(transaction)
                  .flatMap(
                      saved ->
                          transactionKafkaPublisher
                              .publishTransactionExecuted(saved)
                              .andThen(Single.just(saved)));
            });
  }

  @Override
  public Single<TransactionSaga> handlePaymentRejected(PaymentRejectedEvent event) {
    if (event.getExternalReference() == null || event.getExternalReference().isBlank()) {
      return Single.error(
          new IllegalArgumentException("PaymentRejectedEvent recibido sin externalReference."));
    }

    return getTransaction(event.getExternalReference())
        .flatMap(
            transaction -> {
              if (transaction.getStatus() != SagaStatus.CREDITING_DESTINATION) {
                log.info(
                    "Ignorando PaymentRejectedEvent para transaccion {} en estado {}",
                    transaction.getId(),
                    transaction.getStatus());
                return Single.just(transaction);
              }

              return compensateSourceAccount(
                  transaction, new IllegalArgumentException(event.getErrorMessage()));
            });
  }

  private Single<TransactionSaga> createSaga(
      TransactionType type, String sourceId, String targetId, BigDecimal amount) {
    TransactionSaga transactionSaga =
        TransactionSaga.builder()
            .type(type)
            .sourceId(sourceId)
            .targetId(targetId)
            .amount(amount)
            .status(SagaStatus.CREATED)
            .createdAt(LocalDateTime.now())
            .updatedAt(LocalDateTime.now())
            .build();

    return transactionRepository.save(transactionSaga);
  }

  private Single<TransactionSaga> transition(
      TransactionSaga transactionSaga, SagaStatus status, String errorMessage) {
    transactionSaga.setStatus(status);
    transactionSaga.setErrorMessage(errorMessage);
    transactionSaga.setUpdatedAt(LocalDateTime.now());
    if (status != SagaStatus.COMPLETED) {
      transactionSaga.setCompletedAt(null);
    }
    return transactionRepository.save(transactionSaga);
  }

  private Single<TransactionSaga> completeSaga(TransactionSaga transactionSaga) {
    transactionSaga.setStatus(SagaStatus.COMPLETED);
    transactionSaga.setErrorMessage(null);
    transactionSaga.setUpdatedAt(LocalDateTime.now());
    transactionSaga.setCompletedAt(LocalDateTime.now());

    return transactionRepository
        .save(transactionSaga)
        .flatMap(
            saved ->
                transactionKafkaPublisher
                    .publishTransactionExecuted(saved)
                    .andThen(Single.just(saved)));
  }

  private Single<TransactionSaga> failSaga(TransactionSaga transactionSaga, Throwable error) {
    return transition(transactionSaga, SagaStatus.FAILED, resolveErrorMessage(error));
  }

  private Single<TransactionSaga> compensateSourceAccount(
      TransactionSaga transactionSaga, Throwable error) {
    return transition(transactionSaga, SagaStatus.FAILED_COMPENSATING, resolveErrorMessage(error))
        .flatMap(
            failed ->
                accountClient
                    .compensateDeposit(failed.getSourceId(), failed.getAmount())
                    .map(ignored -> failed)
                    .onErrorResumeNext(
                        compensationError -> {
                          failed.setErrorMessage(
                              failed.getErrorMessage()
                                  + " | Compensacion manual requerida: "
                                  + resolveErrorMessage(compensationError));
                          failed.setUpdatedAt(LocalDateTime.now());
                          return transactionRepository.save(failed);
                        }));
  }

  private LocalDateTime resolveEventTime(LocalDateTime eventTime) {
    return eventTime == null ? LocalDateTime.now() : eventTime;
  }

  private void validateDistinctEndpoints(String sourceId, String targetId, String operationName) {
    if (sourceId != null && sourceId.equals(targetId)) {
      throw new IllegalArgumentException(
          "No se puede ejecutar la "
              + operationName
              + " sobre el mismo producto de origen y destino.");
    }
  }

  private String resolveErrorMessage(Throwable error) {
    if (error == null || error.getMessage() == null || error.getMessage().isBlank()) {
      return "Error no controlado en la orquestacion de la transaccion.";
    }
    return error.getMessage();
  }
}
