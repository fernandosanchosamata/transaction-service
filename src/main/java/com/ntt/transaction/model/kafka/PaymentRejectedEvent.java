package com.ntt.transaction.model.kafka;

import java.time.LocalDateTime;
import lombok.Data;

@Data
public class PaymentRejectedEvent {
  private String creditId;
  private String externalReference;
  private String errorMessage;
  private LocalDateTime rejectedAt;
}
