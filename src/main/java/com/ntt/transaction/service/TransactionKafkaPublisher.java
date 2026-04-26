package com.ntt.transaction.service;

import com.ntt.transaction.model.entity.TransactionSaga;
import io.reactivex.rxjava3.core.Completable;

public interface TransactionKafkaPublisher {

  Completable publishApplyPaymentCommand(TransactionSaga transactionSaga);

  Completable publishTransactionExecuted(TransactionSaga transactionSaga);
}
