# 🔗 Question 3: URL Shortener (Bitly)

**Subject**: System Design | **Subject Code**: TCS-504  
**Author**: Anubhav Singh | **Institution**: Graphic Era Hill University  
**Faculty Mentor**: Himanshu Namdev  


[![System Design](https://img.shields.io/badge/System%20Design-HLD-orange?style=for-the-badge)](https://github.com/Anubhav-Singh-1234/system-design-hld-assignment)
[![Performance](https://img.shields.io/badge/Latency-%3C15ms%20Redirect-brightgreen?style=for-the-badge)](https://github.com/Anubhav-Singh-1234/system-design-hld-assignment)
[![Storage](https://img.shields.io/badge/Capacity-3.52%20Trillion%20URLs-blue?style=for-the-badge)](https://github.com/Anubhav-Singh-1234/system-design-hld-assignment)

A highly scalable, ultra-low latency distributed URL shortening platform engineered to handle billions of redirects with Base62 token generation, in-memory caching, and asynchronous Kafka click analytics.

---

## 📌 1. Requirements Engineering

### Functional Requirements (FR)
* **FR1 — URL Shortening**: Accepts an arbitrary long URL and generates a compact, unique, URL-safe short link (e.g. `https://sho.rt/aZ9k2X`).
* **FR2 — High-Speed Redirection**: Navigating to the short URL instantly redirects the user to the destination long URL.
* **FR3 — Persistent Mapping**: Reliable bidirectional storage of short keys and original URLs with optional expiration dates (TTL).
* **FR4 — Real-Time Click Analytics**: Records every click event (timestamp, IP, location, user-agent, referrer) without increasing redirect latency.
* **FR5 — Custom Vanity URLs (Bonus)**: Allows users to specify customized vanity aliases (e.g., `sho.rt/system-design`).

### Non-Functional Requirements (NFR)
* **Ultra-Low Latency Redirection**: Target redirection latency $< 15\text{ ms}$.
* **Extreme Read-to-Write Ratio**: System is designed for a **100:1 Read-to-Write ratio**.
* **High Availability**: 99.999% availability; redirect service must never go down.
* **Unpredictability**: Short URL keys must not be guessable sequentially to prevent scraping attacks.

---

## 🏛️ 2. High-Level Architecture

![URL Shortener HLD](03_URL_Shortener_HLD.png)

<details>
<summary><b>View Architecture Source (Mermaid)</b></summary>

```mermaid
graph TD
    subgraph Clients["Users & Clients"]
        CREATOR["URL Creator / API Client<br/>(Write Traffic)"]
        CLICKER["End-User / Web Browser<br/>(Read Traffic)"]
    end

    LB["Global Anycast DNS & Cloud Load Balancer"]
    CREATOR --> LB
    CLICKER --> LB

    GW["API Gateway & Rate Limiter<br/>SSL Termination, WAF & Routing"]
    LB --> GW

    subgraph Core_Services["Core Application Services"]
        WRITE_SVC["URL Shortener Service<br/>(Write Path - Low Volume)"]
        READ_SVC["High-Performance Redirection Service<br/>(Read Path - High Volume)"]
        KGS["Key Generation Service (KGS)<br/>Pre-generates Unique Base62 Tokens"]
    end

    GW -->|"POST /api/v1/shorten"| WRITE_SVC
    GW -->|"GET /:short_key"| READ_SVC
    WRITE_SVC -->|"Fetch Token Buffer"| KGS

    subgraph Storage_Caching["Caching & Persistence Tier"]
        CACHE[("Redis In-Memory Distributed Cache<br/>LRU Eviction / High Hit-Ratio")]
        URL_DB[("URL Relational Database<br/>PostgreSQL / CockroachDB (Partitioned)")]
        KGS_DB[("KGS Token Store<br/>MySQL / Range Key Allocator")]
    end

    WRITE_SVC -->|"Persist New URL Mapping"| URL_DB
    WRITE_SVC -->|"Pre-warm Cache"| CACHE
    KGS -->|"Allocate Token Ranges"| KGS_DB

    READ_SVC -->|"1. Cache Lookup"| CACHE
    CACHE -.->|"Cache Miss"| READ_SVC
    READ_SVC -->|"2. Query Primary DB on Miss"| URL_DB
    READ_SVC -.->|"3. Write Back to Cache"| CACHE

    subgraph Analytics_Stream["Asynchronous Click Analytics Engine"]
        STREAM{{"Kafka / Redis Stream<br/>Non-blocking Click Event Stream"}}
        AGGREGATOR["Analytics Aggregation Workers"]
        ANALYTICS_DB[("Click Analytics Columnar DB<br/>ClickHouse / TimescaleDB")]
    end

    READ_SVC -->|"Emit Click Event (Async)"| STREAM
    STREAM --> AGGREGATOR
    AGGREGATOR --> ANALYTICS_DB
```
</details>

---

## 🔢 3. Key Generation Strategy: Base62 & KGS

### Mathematical Capacity
Using Base62 characters: `[0-9]`, `[a-z]`, `[A-Z]` ($10 + 26 + 26 = 62\text{ characters}$):
$$\text{Total Capacity for 7 Characters} = 62^7 = 3,521,614,606,208 \approx 3.52\text{ Trillion URLs}$$
Even at **1,000 new URLs created per second**, this keyspace will sustain over **111 years** of continuous generation without exhaustion!

### Comparison of Generation Approaches

| Generation Method | Pros | Cons | Verdict |
| :--- | :--- | :--- | :---: |
| **MD5 / SHA-256 + Truncation** | Deterministic mapping. | Suffers from **hash collisions**; requires expensive DB lookups on insert. | ❌ Suboptimal |
| **Auto-Increment Counter + Base62** | Zero collisions; very fast. | **Predictable** (`abc1`, `abc2`), allowing adversaries to crawl all links. | ⚠️ Security risk |
| **Dedicated Key Generation Service (KGS)** | **$O(1)$ zero-collision allocation**, completely non-predictable, zero DB locking on write. | Requires maintaining a separate key pool service. | ✅ **Selected Choice** |

---

## 🗄️ 4. Database Schema & Cache Design

### 1. URL Mappings Table (Relational or NoSQL)
```sql
CREATE TABLE url_mappings (
    short_key VARCHAR(10) PRIMARY KEY,     -- e.g. 'aZ9k2X'
    long_url TEXT NOT NULL,
    user_id UUID,                          -- NULL for anonymous links
    click_count BIGINT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE DEFAULT NULL
);

CREATE INDEX idx_expires_at ON url_mappings(expires_at);
CREATE INDEX idx_user_id ON url_mappings(user_id);
```

### 2. Redis Caching Strategy (Cache-Aside Pattern)
* **Hit Ratio**: Expected $\ge 90\%$ for hot URLs.
* **Storage Key**: `url:{short_key}` $\rightarrow$ `long_url` string.
* **Eviction Policy**: **allkeys-lru** (Least Recently Used) ensures stale URLs make room for viral links in memory.

---

## 🔀 5. Redirection Workflow: HTTP 301 vs 302 Deep Dive

```mermaid
sequenceDiagram
    autonumber
    actor User as User Browser
    participant RedirSvc as Redirection Service
    participant Cache as Redis Cache
    participant DB as URL Database
    participant Kafka as Kafka Click Stream

    User->>RedirSvc: GET https://sho.rt/aZ9k2X
    RedirSvc->>Cache: GET url:aZ9k2X
    alt Cache Hit (90% of requests)
        Cache-->>RedirSvc: Return long_url (<2ms)
    else Cache Miss
        RedirSvc->>DB: SELECT long_url FROM url_mappings WHERE short_key='aZ9k2X'
        DB-->>RedirSvc: Return long_url
        RedirSvc->>Cache: SETEX url:aZ9k2X 604800 long_url
    end

    par Redirect Browser
        RedirSvc-->>User: HTTP 302 Found (Location: https://original-url.com)
    and Non-Blocking Click Tracking
        RedirSvc-)Kafka: Produce ClickEvent {short_key, IP, UserAgent, Timestamp}
    end
```

### Why `HTTP 302 Found` instead of `301 Moved Permanently`?
* **`301 Moved Permanently`**: The browser caches the redirect mapping permanently. Subsequent clicks bypass the URL shortener entirely.
  - *Major Flaw*: **Analytics tracking is completely broken**, making click counting impossible!
* **`302 Found`**: The browser requests the shortener on every visit.
  - *Key Benefit*: **100% accurate real-time analytics**, click counts, and geo-tracking on every click.
  - *Performance*: Sub-15 ms redirect maintained effortlessly via Redis RAM cache.

---

## 📊 6. Asynchronous Click Tracking Pipeline

1. **Non-Blocking Ingestion**: When redirecting, the Redirection Service dispatches a lightweight JSON event to an **Apache Kafka** topic in background without waiting.
2. **Batch Aggregation**: Dedicated analytics workers consume batches of 500 events from Kafka.
3. **Columnar Ingestion**: Events are bulk-written to **ClickHouse** or **TimescaleDB**, providing rapid aggregation queries for analytics dashboards (e.g. clicks by country, browser, referrer).
