package com.ntt.transaction.model.kafka;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionExecutedEvent {
  private String transactionId;
  private String type;
  private String sourceId;
  private String targetId;
  private BigDecimal amount;
  private String status;
  private LocalDateTime executedAt;
}
