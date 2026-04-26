package com.ntt.transaction.model.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import lombok.Data;

@Data
public class PaymentRequest {

  @NotBlank(message = "El sourceId es obligatorio")
  private String sourceId;

  @NotBlank(message = "El targetId es obligatorio")
  private String targetId;

  @NotNull(message = "El amount es obligatorio")
  @DecimalMin(value = "0.0", inclusive = false, message = "El amount debe ser mayor a 0")
  private BigDecimal amount;
}
