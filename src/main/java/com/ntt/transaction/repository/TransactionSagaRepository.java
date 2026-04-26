package com.ntt.transaction.repository;

import com.ntt.transaction.model.entity.TransactionSaga;
import io.reactivex.rxjava3.core.Flowable;
import org.springframework.data.repository.reactive.RxJava3CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TransactionSagaRepository extends RxJava3CrudRepository<TransactionSaga, String> {
  Flowable<TransactionSaga> findBySourceId(String sourceId);
}
