package com.ntt.transaction.controller;

import com.ntt.transaction.model.dto.PaymentRequest;
import com.ntt.transaction.model.dto.TransferRequest;
import com.ntt.transaction.model.entity.TransactionSaga;
import com.ntt.transaction.service.TransactionService;
import io.reactivex.rxjava3.core.Single;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/transactions")
@RequiredArgsConstructor
public class TransactionController {

  private final TransactionService transactionService;

  @PostMapping("/transfers")
  public Single<ResponseEntity<TransactionSaga>> transfer(
      @Valid @RequestBody TransferRequest request) {
    return transactionService.transfer(request).map(ResponseEntity::ok);
  }

  @PostMapping("/payments")
  public Single<ResponseEntity<TransactionSaga>> pay(@Valid @RequestBody PaymentRequest request) {
    return transactionService
        .pay(request)
        .map(response -> ResponseEntity.status(HttpStatus.ACCEPTED).body(response));
  }

  @GetMapping("/{id}")
  public Single<ResponseEntity<TransactionSaga>> getTransaction(@PathVariable String id) {
    return transactionService.getTransaction(id).map(ResponseEntity::ok);
  }
}
