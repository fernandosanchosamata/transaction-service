package com.ntt.transaction.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ntt.transaction.model.dto.PaymentRequest;
import com.ntt.transaction.model.dto.TransferRequest;
import com.ntt.transaction.model.entity.TransactionSaga;
import com.ntt.transaction.model.enums.SagaStatus;
import com.ntt.transaction.model.enums.TransactionType;
import com.ntt.transaction.service.TransactionService;
import io.reactivex.rxjava3.core.Single;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
class TransactionControllerTest {

  @Mock private TransactionService transactionService;

  @InjectMocks private TransactionController controller;

  @Test
  void transferReturnsOkResponse() {
    TransferRequest request = new TransferRequest();
    request.setSourceAccountId("account-1");
    request.setTargetAccountId("account-2");
    request.setAmount(BigDecimal.valueOf(40));
    TransactionSaga saga = transactionSaga(TransactionType.TRANSFER);
    when(transactionService.transfer(request)).thenReturn(Single.just(saga));

    var result = controller.transfer(request).blockingGet();

    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(result.getBody()).isEqualTo(saga);
    verify(transactionService).transfer(request);
  }

  @Test
  void payReturnsAcceptedResponse() {
    PaymentRequest request = new PaymentRequest();
    request.setSourceId("account-1");
    request.setTargetId("credit-1");
    request.setAmount(BigDecimal.valueOf(70));
    TransactionSaga saga = transactionSaga(TransactionType.PAYMENT);
    when(transactionService.pay(request)).thenReturn(Single.just(saga));

    var result = controller.pay(request).blockingGet();

    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(result.getBody()).isEqualTo(saga);
  }

  @Test
  void getTransactionReturnsOkResponse() {
    TransactionSaga saga = transactionSaga(TransactionType.TRANSFER);
    when(transactionService.getTransaction("tx-1")).thenReturn(Single.just(saga));

    var result = controller.getTransaction("tx-1").blockingGet();

    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(result.getBody()).isEqualTo(saga);
  }

  private TransactionSaga transactionSaga(TransactionType type) {
    return TransactionSaga.builder()
        .id("tx-1")
        .type(type)
        .sourceId("account-1")
        .targetId("target-1")
        .amount(BigDecimal.valueOf(50))
        .status(SagaStatus.COMPLETED)
        .build();
  }
}
