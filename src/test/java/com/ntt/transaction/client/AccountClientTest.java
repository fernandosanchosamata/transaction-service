package com.ntt.transaction.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ntt.transaction.model.dto.AccountOperationRequest;
import com.ntt.transaction.model.dto.AccountResponse;
import java.math.BigDecimal;
import java.util.function.Function;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings({"rawtypes", "unchecked"})
class AccountClientTest {

  @Mock private WebClient.Builder webClientBuilder;
  @Mock private WebClient webClient;
  @Mock private WebClient.RequestBodyUriSpec requestBodyUriSpec;
  @Mock private WebClient.RequestBodySpec requestBodySpec;
  @Mock private WebClient.RequestHeadersSpec requestHeadersSpec;
  @Mock private WebClient.ResponseSpec responseSpec;

  private AccountClient client;

  @BeforeEach
  void setUp() {
    client = new AccountClient(webClientBuilder);
  }

  @Test
  void withdrawInvokesAccountWithdrawEndpoint() {
    AccountResponse response = accountResponse();
    stubAccountOperation(
        "http://ACCOUNT-SERVICE/api/v1/accounts/account-1/withdraw", BigDecimal.TEN, response);

    AccountResponse result = client.withdraw("account-1", BigDecimal.TEN).blockingGet();

    assertThat(result).isSameAs(response);
    verify(requestBodySpec).bodyValue(any(AccountOperationRequest.class));
  }

  @Test
  void depositInvokesAccountDepositEndpoint() {
    AccountResponse response = accountResponse();
    stubAccountOperation(
        "http://ACCOUNT-SERVICE/api/v1/accounts/account-1/deposit", BigDecimal.ONE, response);

    AccountResponse result = client.deposit("account-1", BigDecimal.ONE).blockingGet();

    assertThat(result).isSameAs(response);
  }

  @Test
  void compensateDepositInvokesAccountCompensationEndpoint() {
    AccountResponse response = accountResponse();
    stubAccountOperation(
        "http://ACCOUNT-SERVICE/api/v1/accounts/account-1/compensations/deposit",
        BigDecimal.valueOf(5),
        response);

    AccountResponse result =
        client.compensateDeposit("account-1", BigDecimal.valueOf(5)).blockingGet();

    assertThat(result).isSameAs(response);
  }

  private void stubAccountOperation(String uri, BigDecimal amount, AccountResponse response) {
    when(webClientBuilder.build()).thenReturn(webClient);
    when(webClient.post()).thenReturn(requestBodyUriSpec);
    when(requestBodyUriSpec.uri(uri)).thenReturn(requestBodySpec);
    when(requestBodySpec.bodyValue(any(AccountOperationRequest.class)))
        .thenReturn(requestHeadersSpec);
    when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
    when(responseSpec.onStatus(any(Predicate.class), any(Function.class))).thenReturn(responseSpec);
    when(responseSpec.bodyToMono(AccountResponse.class)).thenReturn(Mono.just(response));
  }

  private AccountResponse accountResponse() {
    AccountResponse response = new AccountResponse();
    response.setId("account-1");
    response.setBalance(BigDecimal.valueOf(100));
    return response;
  }
}
