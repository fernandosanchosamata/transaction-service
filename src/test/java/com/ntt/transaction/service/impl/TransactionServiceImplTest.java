package com.ntt.transaction.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ntt.transaction.client.AccountClient;
import com.ntt.transaction.model.dto.AccountResponse;
import com.ntt.transaction.model.dto.PaymentRequest;
import com.ntt.transaction.model.dto.TransferRequest;
import com.ntt.transaction.model.entity.TransactionSaga;
import com.ntt.transaction.model.enums.SagaStatus;
import com.ntt.transaction.model.enums.TransactionType;
import com.ntt.transaction.repository.TransactionSagaRepository;
import com.ntt.transaction.service.TransactionKafkaPublisher;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import java.math.BigDecimal;
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
    when(transactionRepository.save(any(TransactionSaga.class)))
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

  private AccountResponse accountResponse(String accountId) {
    AccountResponse response = new AccountResponse();
    response.setId(accountId);
    response.setCustomerId("customer-1");
    response.setBalance(BigDecimal.valueOf(100));
    response.setStatus("ACTIVE");
    return response;
  }
}
