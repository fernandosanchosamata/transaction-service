package com.ntt.transaction.controller;

import com.ntt.transaction.model.dto.PaymentRequest;
import com.ntt.transaction.model.dto.TransferRequest;
import com.ntt.transaction.model.entity.TransactionSaga;
import com.ntt.transaction.service.TransactionService;
import io.reactivex.rxjava3.core.Single;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class TransactionController {

  private final TransactionService transactionService;

  @PostMapping("/transfers")
  public Single<ResponseEntity<TransactionSaga>> transfer(
      @Valid @RequestBody TransferRequest request) {
    log.info(
        "Solicitud recibida para transferencia. sourceId={}, targetId={}",
        request.getSourceAccountId(),
        request.getTargetAccountId());
    return transactionService
        .transfer(request)
        .doOnSuccess(
            saga ->
                log.info(
                    "Transferencia procesada. transactionId={}, status={}",
                    saga.getId(),
                    saga.getStatus()))
        .doOnError(error -> log.warn("No se pudo procesar transferencia: {}", error.getMessage()))
        .map(ResponseEntity::ok);
  }

  @PostMapping("/payments")
  public Single<ResponseEntity<TransactionSaga>> pay(@Valid @RequestBody PaymentRequest request) {
    log.info(
        "Solicitud recibida para pago. sourceId={}, targetId={}",
        request.getSourceId(),
        request.getTargetId());
    return transactionService
        .pay(request)
        .doOnSuccess(
            saga ->
                log.info(
                    "Pago aceptado. transactionId={}, status={}", saga.getId(), saga.getStatus()))
        .doOnError(error -> log.warn("No se pudo procesar pago: {}", error.getMessage()))
        .map(response -> ResponseEntity.status(HttpStatus.ACCEPTED).body(response));
  }

  @GetMapping("/{id}")
  public Single<ResponseEntity<TransactionSaga>> getTransaction(@PathVariable String id) {
    log.info("Solicitud recibida para consultar transaccion. transactionId={}", id);
    return transactionService
        .getTransaction(id)
        .doOnSuccess(
            saga ->
                log.info(
                    "Transaccion consultada. transactionId={}, status={}",
                    saga.getId(),
                    saga.getStatus()))
        .doOnError(error -> log.warn("No se pudo consultar transaccion: {}", error.getMessage()))
        .map(ResponseEntity::ok);
  }
}
