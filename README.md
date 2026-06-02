# fwp-route-stream

A Spring Boot 4 Kafka Streams microservice that consumes flight route events, enriches and deduplicates them, persists to PostgreSQL, and fans out to a downstream topic.

---

## Architecture

```
routes (Kafka topic)
    │
    ▼
[parse JSON] → [filter invalid] → [enrich departure time] → [selectKey SHA-256]
    │
    ▼
[repartition] → [toTable (KTable — dedup)] → [toStream]
    │                                              │
    ▼                                              ▼
[persist → PostgreSQL]               [routes-ag (Kafka topic)]
```

**Enrichment rules:**
- Departure time `null` → set to today @ 06:00
- Departure time present → floor to nearest 15-minute slot (e.g. 14:37 → 14:30)
- Format: `yyyy-MM-dd HH:mm`

**Identity key:** SHA-256 of `airline|SOURCE|DESTINATION|departureTime`, truncated to 32 hex chars.  
The KTable collapses duplicate Kafka deliveries before they reach the DB sink.

---

## Tech Stack

| Layer | Library |
|-------|---------|
| Runtime | Java 21, Spring Boot 4.0.6 |
| Streaming | Confluent Kafka 7.9.0-ccs (≈ Apache Kafka 3.9.x), Kafka Streams |
| State store | RocksDB 9.7.3 |
| Persistence | Spring Data JPA, Hibernate 6, Hibernate Envers (audit) |
| DB migrations | Flyway |
| Database | PostgreSQL |
| Observability | Spring Boot Actuator |

---

## Profiles

| Profile | Kafka | Database | DDL |
|---------|-------|----------|-----|
| `local` (default) | `localhost:9092` (Confluent Docker) | `localhost:5432/fwf_demo_db` | Hibernate `update` |
| `ec2` | MSK via `$KAFKA_BOOTSTRAP_SERVERS` (SASL_SSL) | RDS via `$DB_URL` | Flyway migrations |

---

## Running Locally

**Prerequisites:** Docker, Java 21

```bash
# 1. Start Kafka
docker run -d -p 9092:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  confluentinc/cp-kafka:7.9.0

# 2. Start PostgreSQL
docker run -d -p 5432:5432 \
  -e POSTGRES_DB=fwf_demo_db \
  -e POSTGRES_USER=myuser \
  -e POSTGRES_PASSWORD=secret \
  postgres:16

# 3. Run the app (local profile active by default)
./gradlew bootRun
```

The app creates the `routes` and `routes-ag` Kafka topics on first use (auto-create must be enabled on the broker).  
DB tables are created by Hibernate on startup (`ddl-auto=update`).

---

## Running on EC2 / AWS

```bash
export KAFKA_BOOTSTRAP_SERVERS=<msk-broker>:9092
export KAFKA_API_KEY=<key>
export KAFKA_API_SECRET=<secret>
export DB_URL=jdbc:postgresql://<rds-endpoint>:5432/fwp
export DB_USERNAME=<user>
export DB_PASSWORD=<pass>

java -jar fwp-route-stream-0.0.1-SNAPSHOT.jar \
     --spring.profiles.active=ec2
```

Flyway runs `V1__create_routes_table.sql` and `V2__create_envers_audit_tables.sql` before Hibernate validates the schema.

---

## Kafka Topic Configuration

External topics (`routes`, `routes-ag`) must be created with:

```
replication.factor=3
min.insync.replicas=2
```

Producer is configured with `acks=all` and `lz4` compression.  
For local single-broker: overrides in `application-local.properties` set both to `1`.

---

## MSK IAM Auth (alternative to SASL/PLAIN)

Replace the four SASL lines in `application-ec2.properties` with:

```properties
spring.kafka.streams.properties.sasl.mechanism=AWS_MSK_IAM
spring.kafka.streams.properties.sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
spring.kafka.streams.properties.sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

And add to `build.gradle`: `implementation 'software.amazon.msk:aws-msk-iam-auth:2.2.0'`

---

## Input Message Format

Topic `routes`, value is JSON:

```json
{
  "airline": "BA",
  "sourceAirport": "LHR",
  "destinationAirport": "JFK",
  "codeShare": "Y",
  "stops": 0,
  "equipment": "77W",
  "departureTime": "2025-06-01 14:37"
}
```

`departureTime` is optional. All other fields are nullable; records missing `airline`, `sourceAirport`, or `destinationAirport` are filtered out.

---

## Tests

```bash
./gradlew test
```

8 unit tests via `TopologyTestDriver` (no broker required):

| Test | Covers |
|------|--------|
| `happyPath` | Valid route → DB + routes-ag |
| `deduplication` | Same key twice → single DB call |
| `nullDepartureTime` | Null → today@06:00 |
| `departureTimeFlooring` | 14:37 → 14:30 |
| `fifteenMinuteBoundary` | 14:30 unchanged |
| `malformedJson` | Unparseable → filtered, no output |
| `blankFields` | Missing required fields → filtered |
| `tombstone` | Null value → filtered, no output |

---

## Actuator

```
GET /actuator/health
GET /actuator/info
GET /actuator/metrics
```

---

## Project Structure

```
src/main/java/org/fwp/route/stream/
├── config/
│   ├── KafkaConfig.java          # defaultKafkaStreamsConfig bean (Spring Boot 4.x explicit wiring)
│   ├── AuditConfig.java          # JPA auditing + Envers
│   └── AuditRevisionListener.java
├── topology/
│   └── RouteTopology.java        # Kafka Streams DSL pipeline
├── service/
│   └── RouteService.java         # enrich(), keyOf(), upsert()
├── entity/
│   ├── RouteEntity.java          # JPA entity (@Audited)
│   └── AuditRevisionEntity.java  # Envers revision entity
├── model/
│   ├── Route.java                # Immutable Java record (wire model)
│   └── RouteAggregate.java
├── serde/
│   └── JsonSerde.java            # Generic Jackson Serde
├── properties/
│   └── AppProperties.java        # @ConfigurationProperties for topic names
└── repository/
    └── RouteRepository.java

src/main/resources/
├── application.properties         # Common config
├── application-local.properties   # Local overrides (gitignored)
├── application-ec2.properties     # AWS/MSK config (env-var driven)
└── db/migration/
    ├── V1__create_routes_table.sql
    └── V2__create_envers_audit_tables.sql
```