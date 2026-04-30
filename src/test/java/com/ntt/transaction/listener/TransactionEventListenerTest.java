package com.ntt.transaction.listener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ntt.transaction.config.TransactionKafkaProperties;
import com.ntt.transaction.model.entity.TransactionSaga;
import com.ntt.transaction.model.enums.SagaStatus;
import com.ntt.transaction.model.enums.TransactionType;
import com.ntt.transaction.model.kafka.PaymentAppliedEvent;
import com.ntt.transaction.model.kafka.PaymentRejectedEvent;
import com.ntt.transaction.service.TransactionService;
import io.reactivex.rxjava3.core.Single;
import java.math.BigDecimal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TransactionEventListenerTest {

  @Mock private TransactionService transactionService;

  private TransactionEventListener listener;
  private ObjectMapper objectMapper;

  @BeforeEach
  void setUp() {
    objectMapper = new ObjectMapper();
    listener =
        new TransactionEventListener(
            objectMapper, transactionService, new TransactionKafkaProperties());
  }

  @Test
  void handlePaymentAppliedDelegatesValidEventToService() throws Exception {
    PaymentAppliedEvent event = new PaymentAppliedEvent();
    event.setExternalReference("tx-1");
    event.setCreditId("credit-1");
    event.setAmount(BigDecimal.TEN);
    when(transactionService.handlePaymentApplied(any()))
        .thenReturn(Single.just(saga(TransactionType.PAYMENT)));

    listener.handlePaymentApplied(objectMapper.writeValueAsString(event));

    verify(transactionService).handlePaymentApplied(any(PaymentAppliedEvent.class));
  }

  @Test
  void handlePaymentRejectedDelegatesValidEventToService() throws Exception {
    PaymentRejectedEvent event = new PaymentRejectedEvent();
    event.setExternalReference("tx-1");
    event.setCreditId("credit-1");
    event.setErrorMessage("rechazado");
    when(transactionService.handlePaymentRejected(any()))
        .thenReturn(Single.just(saga(TransactionType.PAYMENT)));

    listener.handlePaymentRejected(objectMapper.writeValueAsString(event));

    verify(transactionService).handlePaymentRejected(any(PaymentRejectedEvent.class));
  }

  @Test
  void handlePaymentAppliedIgnoresInvalidPayload() {
    listener.handlePaymentApplied("{invalid-json");

    verify(transactionService, never()).handlePaymentApplied(any());
  }

  @Test
  void handlePaymentRejectedIgnoresInvalidPayload() {
    listener.handlePaymentRejected("{invalid-json");

    verify(transactionService, never()).handlePaymentRejected(any());
  }

  private TransactionSaga saga(TransactionType type) {
    return TransactionSaga.builder()
        .id("tx-1")
        .type(type)
        .sourceId("account-1")
        .targetId("credit-1")
        .amount(BigDecimal.TEN)
        .status(SagaStatus.COMPLETED)
        .build();
  }
}
