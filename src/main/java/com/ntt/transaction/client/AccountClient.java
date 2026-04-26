package com.ntt.transaction.client;

import com.ntt.transaction.model.dto.AccountOperationRequest;
import com.ntt.transaction.model.dto.AccountResponse;
import io.reactivex.rxjava3.core.Single;
import java.math.BigDecimal;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.adapter.rxjava.RxJava3Adapter;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class AccountClient {

  private static final String ACCOUNT_SERVICE_URL = "http://ACCOUNT-SERVICE/api/v1/accounts";

  private final WebClient.Builder webClientBuilder;

  public Single<AccountResponse> withdraw(String accountId, BigDecimal amount) {
    return invokeAccountOperation(accountId + "/withdraw", amount);
  }

  public Single<AccountResponse> deposit(String accountId, BigDecimal amount) {
    return invokeAccountOperation(accountId + "/deposit", amount);
  }

  public Single<AccountResponse> compensateDeposit(String accountId, BigDecimal amount) {
    return invokeAccountOperation(accountId + "/compensations/deposit", amount);
  }

  private Single<AccountResponse> invokeAccountOperation(String path, BigDecimal amount) {
    Mono<AccountResponse> response =
        webClientBuilder
            .build()
            .post()
            .uri(ACCOUNT_SERVICE_URL + "/" + path)
            .bodyValue(new AccountOperationRequest(amount))
            .retrieve()
            .onStatus(
                HttpStatusCode::isError,
                clientResponse ->
                    clientResponse
                        .bodyToMono(String.class)
                        .defaultIfEmpty("Operacion rechazada por Account Service")
                        .flatMap(
                            body ->
                                Mono.error(
                                    new IllegalArgumentException(
                                        "Account Service rechazo la operacion: " + body))))
            .bodyToMono(AccountResponse.class);

    return RxJava3Adapter.monoToSingle(response);
  }
}
