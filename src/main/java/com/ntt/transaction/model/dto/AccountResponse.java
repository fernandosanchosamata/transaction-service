package com.ntt.transaction.model.dto;

import java.math.BigDecimal;
import lombok.Data;

@Data
public class AccountResponse {
  private String id;
  private String customerId;
  private BigDecimal balance;
  private String status;
}
