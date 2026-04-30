package com.ntt.transaction.exception;

import java.time.LocalDateTime;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import reactor.core.publisher.Mono;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

  @ExceptionHandler(WebExchangeBindException.class)
  public Mono<ResponseEntity<ErrorResponse>> handleBindException(WebExchangeBindException ex) {
    String message =
        ex.getBindingResult().getFieldErrors().stream()
            .map(FieldError::getDefaultMessage)
            .collect(Collectors.joining(", "));

    ErrorResponse response =
        ErrorResponse.builder()
            .status(HttpStatus.BAD_REQUEST.value())
            .error(HttpStatus.BAD_REQUEST.getReasonPhrase())
            .message(message)
            .timestamp(LocalDateTime.now())
            .build();

    log.warn("Payload invalido: {}", message);
    return Mono.just(ResponseEntity.badRequest().body(response));
  }

  @ExceptionHandler(IllegalArgumentException.class)
  public Mono<ResponseEntity<ErrorResponse>> handleIllegalArgumentException(
      IllegalArgumentException ex) {
    ErrorResponse response =
        ErrorResponse.builder()
            .status(HttpStatus.BAD_REQUEST.value())
            .error(HttpStatus.BAD_REQUEST.getReasonPhrase())
            .message(ex.getMessage())
            .timestamp(LocalDateTime.now())
            .build();

    log.warn("Solicitud rechazada por regla de negocio: {}", ex.getMessage());
    return Mono.just(ResponseEntity.badRequest().body(response));
  }
}
