package com.ntt.transaction.model.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import lombok.Data;

@Data
public class TransferRequest {

  @NotBlank(message = "El sourceAccountId es obligatorio")
  private String sourceAccountId;

  @NotBlank(message = "El targetAccountId es obligatorio")
  private String targetAccountId;

  @NotNull(message = "El amount es obligatorio")
  @DecimalMin(value = "0.0", inclusive = false, message = "El amount debe ser mayor a 0")
  private BigDecimal amount;
}
