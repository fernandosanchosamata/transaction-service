package com.ntt.transaction.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ntt.transaction.client.AccountClient;
import com.ntt.transaction.model.dto.AccountResponse;
import com.ntt.transaction.model.dto.PaymentRequest;
import com.ntt.transaction.model.dto.TransferRequest;
import com.ntt.transaction.model.entity.TransactionSaga;
import com.ntt.transaction.model.enums.SagaStatus;
import com.ntt.transaction.model.enums.TransactionType;
import com.ntt.transaction.model.kafka.PaymentAppliedEvent;
import com.ntt.transaction.model.kafka.PaymentRejectedEvent;
import com.ntt.transaction.repository.TransactionSagaRepository;
import com.ntt.transaction.service.TransactionKafkaPublisher;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TransactionServiceImplTest {

  @Mock private TransactionSagaRepository transactionRepository;
  @Mock private AccountClient accountClient;
  @Mock private TransactionKafkaPublisher transactionKafkaPublisher;

  private TransactionServiceImpl service;

  @BeforeEach
  void setUp() {
    service =
        new TransactionServiceImpl(transactionRepository, accountClient, transactionKafkaPublisher);
    lenient()
        .when(transactionRepository.save(any(TransactionSaga.class)))
        .thenAnswer(
            invocation -> {
              TransactionSaga saga = invocation.getArgument(0);
              if (saga.getId() == null) {
                saga.setId("tx-1");
              }
              return Single.just(saga);
            });
  }

  @Test
  void transferRejectsSameSourceAndTargetAccount() {
    TransferRequest request = new TransferRequest();
    request.setSourceAccountId("account-1");
    request.setTargetAccountId("account-1");
    request.setAmount(BigDecimal.valueOf(50));

    assertThatThrownBy(() -> service.transfer(request).blockingGet())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "No se puede ejecutar la transferencia sobre el mismo producto de origen y destino.");
    verify(transactionRepository, never()).save(any());
  }

  @Test
  void payRejectsSameSourceAndTargetProduct() {
    PaymentRequest request = new PaymentRequest();
    request.setSourceId("credit-1");
    request.setTargetId("credit-1");
    request.setAmount(BigDecimal.valueOf(70));

    assertThatThrownBy(() -> service.pay(request).blockingGet())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No se puede ejecutar la pago sobre el mismo producto de origen y destino.");
    verify(transactionRepository, never()).save(any());
  }

  @Test
  void transferCompletesWhenWithdrawAndDepositSucceed() {
    TransferRequest request = new TransferRequest();
    request.setSourceAccountId("account-1");
    request.setTargetAccountId("account-2");
    request.setAmount(BigDecimal.valueOf(50));
    when(accountClient.withdraw("account-1", BigDecimal.valueOf(50)))
        .thenReturn(Single.just(accountResponse("account-1")));
    when(accountClient.deposit("account-2", BigDecimal.valueOf(50)))
        .thenReturn(Single.just(accountResponse("account-2")));
    when(transactionKafkaPublisher.publishTransactionExecuted(any(TransactionSaga.class)))
        .thenReturn(Completable.complete());

    TransactionSaga saga = service.transfer(request).blockingGet();

    assertThat(saga.getType()).isEqualTo(TransactionType.TRANSFER);
    assertThat(saga.getStatus()).isEqualTo(SagaStatus.COMPLETED);
    verify(accountClient).withdraw("account-1", BigDecimal.valueOf(50));
    verify(accountClient).deposit("account-2", BigDecimal.valueOf(50));
  }

  @Test
  void transferFailsSagaWhenWithdrawFails() {
    TransferRequest request = new TransferRequest();
    request.setSourceAccountId("account-1");
    request.setTargetAccountId("account-2");
    request.setAmount(BigDecimal.valueOf(50));
    when(accountClient.withdraw("account-1", BigDecimal.valueOf(50)))
        .thenReturn(Single.error(new IllegalArgumentException("withdraw rejected")));

    TransactionSaga saga = service.transfer(request).blockingGet();

    assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED);
    assertThat(saga.getErrorMessage()).isEqualTo("withdraw rejected");
  }

  @Test
  void payPublishesApplyPaymentCommandAndLeavesSagaCreditingDestination() {
    PaymentRequest request = new PaymentRequest();
    request.setSourceId("account-1");
    request.setTargetId("credit-1");
    request.setAmount(BigDecimal.valueOf(70));
    when(accountClient.withdraw("account-1", BigDecimal.valueOf(70)))
        .thenReturn(Single.just(accountResponse("account-1")));
    when(transactionKafkaPublisher.publishApplyPaymentCommand(any(TransactionSaga.class)))
        .thenReturn(Completable.complete());

    TransactionSaga saga = service.pay(request).blockingGet();

    assertThat(saga.getType()).isEqualTo(TransactionType.PAYMENT);
    assertThat(saga.getStatus()).isEqualTo(SagaStatus.CREDITING_DESTINATION);
    verify(transactionKafkaPublisher).publishApplyPaymentCommand(any(TransactionSaga.class));
  }

  @Test
  void payCompensatesWhenPublishApplyPaymentCommandFails() {
    PaymentRequest request = new PaymentRequest();
    request.setSourceId("account-1");
    request.setTargetId("credit-1");
    request.setAmount(BigDecimal.valueOf(70));
    when(accountClient.withdraw("account-1", BigDecimal.valueOf(70)))
        .thenReturn(Single.just(accountResponse("account-1")));
    when(transactionKafkaPublisher.publishApplyPaymentCommand(any(TransactionSaga.class)))
        .thenReturn(Completable.error(new IllegalArgumentException("kafka down")));
    when(accountClient.compensateDeposit("account-1", BigDecimal.valueOf(70)))
        .thenReturn(Single.just(accountResponse("account-1")));

    TransactionSaga saga = service.pay(request).blockingGet();

    assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED_COMPENSATING);
    assertThat(saga.getErrorMessage()).contains("kafka down");
  }

  @Test
  void transferCompensatesSourceWhenTargetDepositFails() {
    TransferRequest request = new TransferRequest();
    request.setSourceAccountId("account-1");
    request.setTargetAccountId("account-2");
    request.setAmount(BigDecimal.valueOf(50));
    when(accountClient.withdraw("account-1", BigDecimal.valueOf(50)))
        .thenReturn(Single.just(accountResponse("account-1")));
    when(accountClient.deposit("account-2", BigDecimal.valueOf(50)))
        .thenReturn(Single.error(new IllegalArgumentException("deposit rejected")));
    when(accountClient.compensateDeposit("account-1", BigDecimal.valueOf(50)))
        .thenReturn(Single.just(accountResponse("account-1")));

    TransactionSaga saga = service.transfer(request).blockingGet();

    assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED_COMPENSATING);
    assertThat(saga.getErrorMessage()).contains("deposit rejected");
    verify(accountClient).compensateDeposit("account-1", BigDecimal.valueOf(50));
  }

  @Test
  void transferMarksManualCompensationWhenCompensationFails() {
    TransferRequest request = new TransferRequest();
    request.setSourceAccountId("account-1");
    request.setTargetAccountId("account-2");
    request.setAmount(BigDecimal.valueOf(50));
    when(accountClient.withdraw("account-1", BigDecimal.valueOf(50)))
        .thenReturn(Single.just(accountResponse("account-1")));
    when(accountClient.deposit("account-2", BigDecimal.valueOf(50)))
        .thenReturn(Single.error(new IllegalArgumentException("deposit rejected")));
    when(accountClient.compensateDeposit("account-1", BigDecimal.valueOf(50)))
        .thenReturn(Single.error(new IllegalArgumentException("compensation rejected")));

    TransactionSaga saga = service.transfer(request).blockingGet();

    assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED_COMPENSATING);
    assertThat(saga.getErrorMessage())
        .contains("deposit rejected")
        .contains("Compensacion manual requerida")
        .contains("compensation rejected");
  }

  @Test
  void getTransactionRejectsMissingTransaction() {
    when(transactionRepository.findById("missing")).thenReturn(Maybe.empty());

    assertThatThrownBy(() -> service.getTransaction("missing").blockingGet())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Transaccion no encontrada.");
  }

  @Test
  void handlePaymentAppliedCompletesCreditingTransaction() {
    TransactionSaga saga = saga(SagaStatus.CREDITING_DESTINATION);
    PaymentAppliedEvent event = new PaymentAppliedEvent();
    event.setExternalReference("tx-1");
    event.setAppliedAt(LocalDateTime.of(2026, 4, 29, 12, 0));
    when(transactionRepository.findById("tx-1")).thenReturn(Maybe.just(saga));
    when(transactionKafkaPublisher.publishTransactionExecuted(any(TransactionSaga.class)))
        .thenReturn(Completable.complete());

    TransactionSaga result = service.handlePaymentApplied(event).blockingGet();

    assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(result.getCompletedAt()).isEqualTo(event.getAppliedAt());
    verify(transactionKafkaPublisher).publishTransactionExecuted(any(TransactionSaga.class));
  }

  @Test
  void handlePaymentAppliedIgnoresTransactionInDifferentStatus() {
    TransactionSaga saga = saga(SagaStatus.COMPLETED);
    PaymentAppliedEvent event = new PaymentAppliedEvent();
    event.setExternalReference("tx-1");
    when(transactionRepository.findById("tx-1")).thenReturn(Maybe.just(saga));

    TransactionSaga result = service.handlePaymentApplied(event).blockingGet();

    assertThat(result).isSameAs(saga);
    verify(transactionKafkaPublisher, never()).publishTransactionExecuted(any());
  }

  @Test
  void handlePaymentAppliedRejectsMissingExternalReference() {
    PaymentAppliedEvent event = new PaymentAppliedEvent();

    assertThatThrownBy(() -> service.handlePaymentApplied(event).blockingGet())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("PaymentAppliedEvent recibido sin externalReference.");
  }

  @Test
  void handlePaymentRejectedCompensatesCreditingTransaction() {
    TransactionSaga saga = saga(SagaStatus.CREDITING_DESTINATION);
    PaymentRejectedEvent event = new PaymentRejectedEvent();
    event.setExternalReference("tx-1");
    event.setErrorMessage("payment rejected");
    when(transactionRepository.findById("tx-1")).thenReturn(Maybe.just(saga));
    when(accountClient.compensateDeposit("account-1", BigDecimal.valueOf(50)))
        .thenReturn(Single.just(accountResponse("account-1")));

    TransactionSaga result = service.handlePaymentRejected(event).blockingGet();

    assertThat(result.getStatus()).isEqualTo(SagaStatus.FAILED_COMPENSATING);
    assertThat(result.getErrorMessage()).contains("payment rejected");
  }

  @Test
  void handlePaymentRejectedIgnoresTransactionInDifferentStatus() {
    TransactionSaga saga = saga(SagaStatus.COMPLETED);
    PaymentRejectedEvent event = new PaymentRejectedEvent();
    event.setExternalReference("tx-1");
    when(transactionRepository.findById("tx-1")).thenReturn(Maybe.just(saga));

    TransactionSaga result = service.handlePaymentRejected(event).blockingGet();

    assertThat(result).isSameAs(saga);
    verify(accountClient, never()).compensateDeposit(any(), any());
  }

  private AccountResponse accountResponse(String accountId) {
    AccountResponse response = new AccountResponse();
    response.setId(accountId);
    response.setCustomerId("customer-1");
    response.setBalance(BigDecimal.valueOf(100));
    response.setStatus("ACTIVE");
    return response;
  }

  private TransactionSaga saga(SagaStatus status) {
    return TransactionSaga.builder()
        .id("tx-1")
        .type(TransactionType.PAYMENT)
        .sourceId("account-1")
        .targetId("credit-1")
        .amount(BigDecimal.valueOf(50))
        .status(status)
        .build();
  }
}
