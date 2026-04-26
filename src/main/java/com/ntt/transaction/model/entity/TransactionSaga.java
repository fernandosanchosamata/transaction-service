package com.ntt.transaction.model.entity;

import com.ntt.transaction.model.enums.SagaStatus;
import com.ntt.transaction.model.enums.TransactionType;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "transactions")
public class TransactionSaga {
  @Id private String id;
  private TransactionType type;
  private String sourceId;
  private String targetId;
  private BigDecimal amount;
  private SagaStatus status;
  private String errorMessage;
  private LocalDateTime createdAt;
  private LocalDateTime updatedAt;
  private LocalDateTime completedAt;
}
