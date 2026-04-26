# transaction-service

Microservicio encargado de operaciones transaccionales. Coordina transferencias
entre cuentas, pagos de creditos y consulta de sagas de transaccion.

## Tecnologias

- Java 17
- Spring Boot 3.2.4
- Spring WebFlux
- Spring Data MongoDB Reactive
- Spring Kafka
- Reactor Kafka
- RxJava 3
- Reactor Adapter
- Spring Cloud Config
- Eureka Client
- Springdoc OpenAPI
- Maven
- JUnit 5
- JaCoCo
- Spotless
- Checkstyle

## Puerto

```text
http://localhost:8084
```

## OpenAPI

```text
http://localhost:8084/swagger-ui.html
http://localhost:8084/v3/api-docs
```

## Levantar sin Docker

Requisitos locales:

- Java 17
- Maven
- MongoDB en `localhost:27017`
- Kafka en `localhost:9092`
- Config Server opcional en `localhost:8888`
- Eureka en `localhost:8761`
- Servicios dependientes disponibles segun la operacion

```powershell
cd .\transaction-service
mvn spring-boot:run
```

## Levantar con Docker

Primero levantar la infraestructura:

```powershell
cd .\infra
docker compose up -d
```

Este proyecto aun no incluye `Dockerfile`. Cuando se agregue, debe usar nombres
de servicio de Docker Compose para MongoDB, Kafka, Config Server, Eureka y los
servicios de negocio que consume.

## Tests

```powershell
cd .\transaction-service
mvn test
```

Si luego se agregan tests de integracion:

```powershell
mvn verify
```

## Formato y Checkstyle

```powershell
mvn spotless:apply
mvn checkstyle:check
```

## JaCoCo

```powershell
mvn test jacoco:report
```

Reporte:

```text
target/site/jacoco/index.html
```

## MongoDB

Base de datos:

```text
ntt_transaction
```

Coleccion principal:

```text
transactions
```

Consulta:

```powershell
mongosh
use ntt_transaction
show collections
db.transactions.find().pretty()
```

Se usa database per service logico: cada microservicio mantiene su propia base de
datos MongoDB.

