package com.ntt.transaction.service;

import com.ntt.transaction.model.dto.PaymentRequest;
import com.ntt.transaction.model.dto.TransferRequest;
import com.ntt.transaction.model.entity.TransactionSaga;
import com.ntt.transaction.model.kafka.PaymentAppliedEvent;
import com.ntt.transaction.model.kafka.PaymentRejectedEvent;
import io.reactivex.rxjava3.core.Single;

public interface TransactionService {

  Single<TransactionSaga> transfer(TransferRequest request);

  Single<TransactionSaga> pay(PaymentRequest request);

  Single<TransactionSaga> getTransaction(String transactionId);

  Single<TransactionSaga> handlePaymentApplied(PaymentAppliedEvent event);

  Single<TransactionSaga> handlePaymentRejected(PaymentRejectedEvent event);
}
