package com.ntt.transaction.model.kafka;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ApplyPaymentCommand {
  private String creditId;
  private BigDecimal amount;
  private String sourceId;
  private String externalReference;
}
