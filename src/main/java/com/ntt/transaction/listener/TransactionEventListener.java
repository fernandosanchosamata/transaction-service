package com.ntt.transaction.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ntt.transaction.config.TransactionKafkaProperties;
import com.ntt.transaction.model.kafka.PaymentAppliedEvent;
import com.ntt.transaction.model.kafka.PaymentRejectedEvent;
import com.ntt.transaction.service.TransactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionEventListener {

  private final ObjectMapper objectMapper;
  private final TransactionService transactionService;
  private final TransactionKafkaProperties kafkaProperties;

  @KafkaListener(
      topics = "#{@transactionKafkaProperties.paymentApplied}",
      groupId = "${spring.kafka.consumer.group-id}")
  public void handlePaymentApplied(String payload) {
    PaymentAppliedEvent event =
        deserialize(payload, PaymentAppliedEvent.class, "PaymentAppliedEvent");
    if (event == null) {
      return;
    }

    transactionService
        .handlePaymentApplied(event)
        .subscribe(
            transaction ->
                log.info(
                    "PaymentAppliedEvent procesado para transaccion {} desde topic {}",
                    transaction.getId(),
                    kafkaProperties.getPaymentApplied()),
            error -> log.error("Error procesando PaymentAppliedEvent: {}", error.getMessage()));
  }

  @KafkaListener(
      topics = "#{@transactionKafkaProperties.paymentRejected}",
      groupId = "${spring.kafka.consumer.group-id}")
  public void handlePaymentRejected(String payload) {
    PaymentRejectedEvent event =
        deserialize(payload, PaymentRejectedEvent.class, "PaymentRejectedEvent");
    if (event == null) {
      return;
    }

    transactionService
        .handlePaymentRejected(event)
        .subscribe(
            transaction ->
                log.info(
                    "PaymentRejectedEvent procesado para transaccion {} desde topic {}",
                    transaction.getId(),
                    kafkaProperties.getPaymentRejected()),
            error -> log.error("Error procesando PaymentRejectedEvent: {}", error.getMessage()));
  }

  private <T> T deserialize(String payload, Class<T> type, String eventName) {
    try {
      return objectMapper.readValue(payload, type);
    } catch (JsonProcessingException error) {
      log.error("No fue posible deserializar {}: {}", eventName, error.getMessage());
      return null;
    }
  }
}
