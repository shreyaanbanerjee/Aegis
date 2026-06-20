<div align="center">

<img src="https://img.shields.io/badge/Java-21-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white" />
<img src="https://img.shields.io/badge/Spring_Boot-3.2-6DB33F?style=for-the-badge&logo=spring-boot" />
<img src="https://img.shields.io/badge/Spring_Cloud_Gateway-Reactive-6DB33F?style=for-the-badge&logo=spring" />
<img src="https://img.shields.io/badge/Redis-Token_Vault-DC382D?style=for-the-badge&logo=redis&logoColor=white" />
<img src="https://img.shields.io/badge/Apache_Kafka-Billing-231F20?style=for-the-badge&logo=apache-kafka" />
<img src="https://img.shields.io/badge/PostgreSQL-Audit_DB-316192?style=for-the-badge&logo=postgresql&logoColor=white" />
<img src="https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white" />

# 🛡️ AEGIS
### Enterprise AI Privacy Gateway & Tokenization Firewall

> **A reactive Spring Cloud Gateway that sits between your internal apps and public LLMs, automatically masking PII before it ever leaves your perimeter — and restoring it invisibly on the way back.**

</div>

---

## 📋 Table of Contents

- [What is Aegis?](#-what-is-aegis)
- [How It Works](#-how-it-works)
- [Architecture Overview](#-architecture-overview)
- [Technology Stack](#-technology-stack)
- [Project Structure](#-project-structure)
- [Local Development Setup](#-local-development-setup)
- [Environment Variables Reference](#-environment-variables-reference)
- [Free-Tier Cloud Deployment](#-free-tier-cloud-deployment)
- [API Reference](#-api-reference)
- [Security Design Decisions](#-security-design-decisions)
- [The Recruiter Showcase UI](#-the-recruiter-showcase-ui)

---

## What is Aegis?

Modern enterprises want to leverage public AI models (ChatGPT, GPT-4, Claude) for internal productivity. But there is a problem:

> **Employees type PII into chat boxes. That PII gets sent to third-party servers. Legal and compliance teams lose sleep.**

**Aegis solves this by acting as a transparent proxy:**

```
Employee's App  ->  [AEGIS GATEWAY]  ->  OpenAI API
                        |
                 +------+----------+
                 | 1. Intercepts   |
                 | 2. Masks PII    |  <- "John Doe" becomes "[PERSON_1]"
                 | 3. Stores map   |  <- Redis: [PERSON_1] -> "John Doe"
                 | 4. Forwards     |  <- OpenAI never sees real PII
                 | 5. Rehydrates   |  <- Response gets "John Doe" back
                 | 6. Bills        |  <- Kafka -> PostgreSQL token tracking
                 +-----------------+
```

The employee gets a normal-looking response. OpenAI only ever processed `[PERSON_1]`. **Zero PII crossed the perimeter.**

---

## How It Works

### Step-by-Step Data Flow

| Step | Component | What Happens |
|------|-----------|--------------|
| **1** | `ApiKeyAuthFilter` | Request arrives with `X-API-Key` header. Gateway validates key against PostgreSQL, resolves department, rejects unknown/inactive keys with `401/403` |
| **2** | `PiiMaskingGatewayFilter` | Request body is buffered reactively. `PiiMaskingService` scans the `messages[].content` fields using regex + name dictionary |
| **3** | `PiiMaskingService` | Replaces PII with deterministic tokens: `john@acme.com` becomes `[EMAIL_1]`, `John Smith` becomes `[PERSON_1]`, `123-45-6789` becomes `[SSN_1]` |
| **4** | `TokenVaultService` | Token-to-PII map stored in Redis as a Hash with **60-second TTL**. Key: `aegis:vault:{correlationId}` |
| **5** | `GatewayConfig` | Sanitized request forwarded to `api.openai.com`. Real OpenAI API key injected by the gateway — internal apps never hold it |
| **6** | `ServerHttpResponseDecorator` | OpenAI response intercepted. `PiiMaskingService.rehydrate()` swaps tokens back. Token map deleted from Redis |
| **7** | `KafkaTemplate` | `TokenUsageEvent` published to Kafka topic `token-usage` (fire-and-forget, non-blocking) |
| **8** | `BillingKafkaConsumer` | Reads event, atomically increments `departments.tokens_used` in PostgreSQL, persists `AuditLog` record |

### PII Detection Capabilities

Aegis detects and masks the following PII types **without any external network calls**:

| Token Type | Example Input | Masked As |
|-----------|--------------|-----------|
| `PERSON` | `John Smith`, `Priya Sharma` | `[PERSON_1]` |
| `EMAIL` | `john@acme.com` | `[EMAIL_1]` |
| `SSN` | `123-45-6789` | `[SSN_1]` |
| `PHONE` | `(555) 867-5309` | `[PHONE_1]` |
| `CC` | `4111111111111111` | `[CC_1]` |
| `IP` | `192.168.1.45` | `[IP_1]` |
| `DATE` | `03/15/1985` | `[DATE_1]` |

---

## Architecture Overview

```
+------------------------------------------------------------------------------+
|                         AEGIS GATEWAY (Spring WebFlux)                        |
|                                                                                |
|  +-------------------------------------------------------------------------+  |
|  |                     Global Filter Chain (ordered)                        |  |
|  |                                                                           |  |
|  |  Order -10: ApiKeyAuthFilter   -> Validates X-API-Key, injects Dept.    |  |
|  |  Order  -5: PiiMaskingFilter   -> Masks request, rehydrates response     |  |
|  |  Order   0: RequestRateLimiter -> Redis token bucket per API key         |  |
|  |  Order   1: SCG Routing        -> Proxies to api.openai.com              |  |
|  +-------------------------------------------------------------------------+  |
|                                                                                |
|  Services:                                                                     |
|  +---------------------+  +--------------------+  +----------------------+  |
|  |  PiiMaskingService  |  |  TokenVaultService  |  |  BillingKafkaConsumer|  |
|  |  (Regex + Dict NLP) |  |  (Redis Hash + TTL) |  |  (Kafka -> PostgreSQL|  |
|  +---------------------+  +--------------------+  +----------------------+  |
+------------------------------------------------------------------------------+
         |                         |                         |
         v                         v                         v
   api.openai.com           Redis (Upstash)        Kafka -> PostgreSQL
                            60s TTL vault           (Neon.tech)
```

---

## Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Runtime** | Java 21, Spring Boot 3.2 | Virtual threads support, latest platform features |
| **Gateway** | Spring Cloud Gateway (WebFlux) | Reactive, non-blocking HTTP proxy |
| **PII Engine** | Regex + Name Dictionary | Local, zero-network-call masking |
| **Token Storage** | Redis (Reactive) via Lettuce | PII vault with TTL, distributed rate limiting |
| **Event Streaming** | Apache Kafka | Asynchronous billing event bus |
| **Database** | PostgreSQL (JPA/Hibernate) | Department management, audit trail |
| **Build** | Maven | Dependency management, multi-stage Docker build |
| **Container** | Docker + Docker Compose | Local dev and production deployment |

---

## Project Structure

```
aegis/
├── src/main/java/com/enterprise/aegis/
│   ├── AegisApplication.java          # Spring Boot entry point
│   │
│   ├── config/
│   │   ├── GatewayConfig.java         # Route definitions (-> api.openai.com)
│   │   ├── InfrastructureConfig.java  # Redis template + ObjectMapper beans
│   │   └── KafkaConfig.java           # Producer (idempotent) + Consumer (manual ack)
│   │
│   ├── filter/
│   │   ├── ApiKeyAuthFilter.java      # GlobalFilter order=-10: auth + dept resolution
│   │   └── PiiMaskingGatewayFilter.java # GlobalFilter order=-5: mask + rehydrate + bill
│   │
│   ├── ratelimit/
│   │   ├── ApiKeyRateLimiter.java     # KeyResolver: rate key = API key
│   │   └── RateLimiterConfig.java     # Token bucket config (replenish/burst)
│   │
│   ├── service/
│   │   ├── PiiMaskingService.java     # Regex + dictionary NLP masking engine
│   │   ├── TokenVaultService.java     # Reactive Redis Hash vault with TTL
│   │   └── BillingKafkaConsumer.java  # Consumes token-usage, writes audit + billing
│   │
│   ├── entity/
│   │   ├── Department.java            # JPA entity: API key, quota, tokens used
│   │   └── AuditLog.java              # JPA entity: immutable per-request audit record
│   │
│   ├── repository/
│   │   ├── DepartmentRepository.java  # Atomic token increment, quota check
│   │   └── AuditLogRepository.java    # Append-only audit queries + billing aggregation
│   │
│   └── dto/
│       └── TokenUsageEvent.java       # Kafka event DTO (no PII, billing metadata only)
│
├── src/main/resources/
│   ├── application.yml                # Full config with env-var injection
│   └── static/
│       └── index.html                 # Recruiter showcase UI (self-contained)
│
├── Dockerfile                         # Multi-stage build (JDK build -> JRE runtime)
├── docker-compose.yml                 # Local: PostgreSQL + Redis + Redpanda
├── pom.xml                            # Maven dependencies
└── README.md
```

---

## Local Development Setup

### Prerequisites

- **Java 21** (use [SDKMAN](https://sdkman.io/): `sdk install java 21-tem`)
- **Maven 3.9+** (`brew install maven`)
- **Docker Desktop** (for local infrastructure)

### 1. Clone and enter the project

```bash
git clone https://github.com/yourname/aegis.git
cd aegis
```

### 2. Start local infrastructure

```bash
docker-compose up -d
```

This starts:
- **PostgreSQL** on `localhost:5432` (DB: `aegis_db`, user: `aegis_user`)
- **Redis** on `localhost:6379`
- **Redpanda** (Kafka-compatible) on `localhost:9092`

Verify services are running:
```bash
docker-compose ps
```

### 3. Set your OpenAI API key

```bash
export OPENAI_API_KEY="sk-your-real-openai-api-key"
```

### 4. Build and run

```bash
mvn clean package -DskipTests
java -jar target/aegis-0.0.1-SNAPSHOT.jar
```

Or with Maven directly:
```bash
mvn spring-boot:run
```

### 5. Seed a demo department

The gateway validates API keys against the PostgreSQL database. Insert a demo record:

```bash
docker exec -it aegis-postgres psql -U aegis_user -d aegis_db \
  -c "INSERT INTO departments (name, api_key, monthly_token_quota, tokens_used, active, created_at, updated_at) \
      VALUES ('Engineering', 'dept_engineering_demo_key_001', 1000000, 0, true, NOW(), NOW());"
```

### 6. Open the Showcase UI

```
http://localhost:8080
```

### 7. Test via cURL

```bash
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dept_engineering_demo_key_001" \
  -d '{
    "model": "gpt-4o-mini",
    "messages": [{
      "role": "user",
      "content": "Summarize the work done by John Smith (john@acme.com, SSN: 123-45-6789) this quarter."
    }]
  }'
```

**What Aegis does behind the scenes:**
1. Validates key -> resolves "Engineering" department
2. Masks: `[PERSON_1]`, `[EMAIL_1]`, `[SSN_1]` sent to OpenAI
3. Stores mapping in Redis with 60s TTL
4. Forwards to OpenAI, receives response
5. Rehydrates: you receive `John Smith` in the final response
6. Kafka billing event published -> PostgreSQL quota updated

---

## Environment Variables Reference

### Required in Production

| Variable | Description | Example |
|----------|-------------|---------|
| `OPENAI_API_KEY` | Your OpenAI API key | `sk-proj-abc123...` |
| `DB_URL` | PostgreSQL JDBC URL | `jdbc:postgresql://ep-xxx.neon.tech/aegis_db?sslmode=require` |
| `DB_USERNAME` | Database username | `aegis_user` |
| `DB_PASSWORD` | Database password | `strong-password-here` |
| `REDIS_HOST` | Redis hostname | `apn1-xxx.upstash.io` |
| `REDIS_PASSWORD` | Redis password | `upstash-token-here` |
| `REDIS_PORT` | Redis port | `6379` |
| `REDIS_SSL_ENABLED` | Enable TLS for Upstash | `true` |
| `KAFKA_BROKERS` | Kafka bootstrap servers | `xxx.upstash.io:9092` |
| `KAFKA_SECURITY_PROTOCOL` | Kafka security protocol | `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | SASL mechanism | `SCRAM-SHA-256` |
| `KAFKA_SASL_JAAS_CONFIG` | Full JAAS config string | `org.apache.kafka.common.security.scram.ScramLoginModule...` |

### Optional Tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | HTTP port to bind |
| `VAULT_TTL_SECONDS` | `60` | How long PII tokens live in Redis |
| `RATE_LIMIT_REPLENISH_RATE` | `10` | Requests per second per department |
| `RATE_LIMIT_BURST_CAPACITY` | `20` | Max burst above replenish rate |
| `LOG_LEVEL` | `INFO` | Aegis log verbosity |
| `OPENAI_BASE_URL` | `https://api.openai.com` | Override for Azure OpenAI or local models |

---

## Free-Tier Cloud Deployment

Aegis is designed to run entirely on free-tier managed services.

### Database: [Neon.tech](https://neon.tech) (Serverless PostgreSQL)

1. Create a free Neon project
2. Copy the connection string
3. Set: `DB_URL=jdbc:postgresql://ep-xxx.neon.tech/neondb?sslmode=require`

### Cache + Rate Limiting: [Upstash](https://upstash.com) (Serverless Redis)

1. Create a Redis database in Upstash (free tier: 10,000 commands/day)
2. Set `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_SSL_ENABLED=true`

### Event Streaming: [Upstash Kafka](https://upstash.com/kafka) (Serverless Kafka)

1. Create a Kafka cluster in Upstash
2. Create topic `token-usage`
3. Set all `KAFKA_*` environment variables from the Upstash dashboard

### Hosting: [Render](https://render.com) or [Railway](https://railway.app)

Connect your GitHub repository, set all environment variables in the dashboard, and deploy. The `Dockerfile` handles everything.

**Sample `.env` file for production:**
```bash
OPENAI_API_KEY=sk-proj-your-key
DB_URL=jdbc:postgresql://ep-xxx.neon.tech/neondb?sslmode=require
DB_USERNAME=neondb_owner
DB_PASSWORD=your-neon-password
REDIS_HOST=apn1-xxx.upstash.io
REDIS_PORT=6379
REDIS_PASSWORD=your-upstash-token
REDIS_SSL_ENABLED=true
KAFKA_BROKERS=xxx.upstash.io:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-256
KAFKA_SASL_JAAS_CONFIG=org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="pass";
```

---

## API Reference

### POST `/v1/chat/completions`

Proxies to OpenAI Chat Completions. All PII in `messages[].content` is automatically masked.

**Headers:**

| Header | Required | Description |
|--------|----------|-------------|
| `X-API-Key` | Yes | Department API key |
| `Content-Type` | Yes | `application/json` |

**Request body:** Standard OpenAI format.

**Error Responses:**

| Status | Meaning |
|--------|---------|
| `401` | Missing `X-API-Key` header |
| `403` | Invalid or inactive API key |
| `429` | Rate limit exceeded |
| `502` | OpenAI upstream error |

### GET `/actuator/health`

Returns `{"status":"UP"}` when the gateway is healthy.

---

## Security Design Decisions

### PII Never Stored Permanently
- Original PII values exist in Redis for **60 seconds maximum** (enforced TTL)
- After rehydration, the vault entry is **proactively deleted**
- `AuditLog` stores only the sanitized (masked) prompt excerpt

### OpenAI API Key Never Exposed to Clients
- Internal apps authenticate with department API keys only
- The gateway injects the real key server-side
- Key rotation is a single environment variable change

### Optimistic Locking on Token Counts
- `Department` entity uses `@Version` for Hibernate optimistic locking
- `incrementTokensUsed` uses a direct atomic JPQL `UPDATE` to prevent stale-read races

### At-Least-Once Kafka Delivery
- Consumer uses `MANUAL_IMMEDIATE` acknowledgment
- Offsets committed only after successful DB write
- Idempotent side effects (counter increment + audit append) make duplicate events safe

### Non-Root Docker Container
- Dockerfile creates a dedicated `aegis` OS user
- Minimizes blast radius if the JVM is compromised

---

## The Recruiter Showcase UI

Aegis includes a self-contained single-page web app served at `http://localhost:8080`.

**Features:**
- Live PII masking preview showing `[PERSON_1]`, `[EMAIL_1]` tokens appear in real time
- Animated gateway terminal with log lines appearing as the request flows through each stage
- Token map table showing every masked entity and its placeholder (mirrors the Redis vault)
- Session statistics: live counters for requests, tokens masked, LLM tokens billed, and latency
- Pipeline flow diagram with animated step indicators
- Kafka event JSON viewer showing the billing payload
- Auto-fallback simulation mode when the gateway is not running locally

No build step required — it is a single `index.html` served by Spring Boot's static file handler.

---

## Quick Start (TL;DR)

```bash
# 1. Start infrastructure
docker-compose up -d

# 2. Seed a department
docker exec -it aegis-postgres psql -U aegis_user -d aegis_db \
  -c "INSERT INTO departments (name, api_key, monthly_token_quota, tokens_used, active, created_at, updated_at) \
      VALUES ('Engineering', 'dept_engineering_demo_key_001', 1000000, 0, true, NOW(), NOW());"

# 3. Run the gateway
export OPENAI_API_KEY=sk-your-key
mvn spring-boot:run

# 4. Open the UI
open http://localhost:8080

# 5. Test via curl
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "X-API-Key: dept_engineering_demo_key_001" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"Review for John Smith (john@acme.com, SSN 123-45-6789)"}]}'
```

---

<div align="center">

Built with Spring Cloud Gateway, Redis, Apache Kafka, and PostgreSQL.

*Zero PII leaves the perimeter.*

</div>