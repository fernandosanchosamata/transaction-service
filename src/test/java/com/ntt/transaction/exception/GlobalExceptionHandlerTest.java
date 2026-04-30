package com.ntt.transaction.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.support.WebExchangeBindException;

@ExtendWith(MockitoExtension.class)
class GlobalExceptionHandlerTest {

  @Mock private WebExchangeBindException bindException;

  private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

  @Test
  void handleBindExceptionReturnsBadRequestWithValidationMessages() {
    BeanPropertyBindingResult bindingResult =
        new BeanPropertyBindingResult(new Object(), "request");
    bindingResult.addError(new FieldError("request", "amount", "El monto es obligatorio"));
    when(bindException.getBindingResult()).thenReturn(bindingResult);

    var response = handler.handleBindException(bindException).block();

    assertThat(response).isNotNull();
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(response.getBody()).isNotNull();
    assertThat(response.getBody().getStatus()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(response.getBody().getError()).isEqualTo(HttpStatus.BAD_REQUEST.getReasonPhrase());
    assertThat(response.getBody().getMessage()).isEqualTo("El monto es obligatorio");
    assertThat(response.getBody().getTimestamp()).isNotNull();
  }

  @Test
  void handleIllegalArgumentExceptionReturnsBadRequest() {
    var response =
        handler
            .handleIllegalArgumentException(new IllegalArgumentException("Transaccion invalida."))
            .block();

    assertThat(response).isNotNull();
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(response.getBody()).isNotNull();
    assertThat(response.getBody().getMessage()).isEqualTo("Transaccion invalida.");
  }
}
