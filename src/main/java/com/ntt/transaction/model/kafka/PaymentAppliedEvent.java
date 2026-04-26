package com.ntt.transaction.model.kafka;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.Data;

@Data
public class PaymentAppliedEvent {
  private String creditId;
  private String customerId;
  private BigDecimal amount;
  private BigDecimal outstandingBalance;
  private String status;
  private String externalReference;
  private LocalDateTime appliedAt;
}
