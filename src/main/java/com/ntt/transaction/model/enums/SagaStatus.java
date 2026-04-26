package com.ntt.transaction.model.enums;

public enum SagaStatus {
  CREATED,
  VALIDATING_FUNDS,
  FUNDS_RESERVED,
  CREDITING_DESTINATION,
  COMPLETED,
  FAILED_COMPENSATING,
  FAILED
}
