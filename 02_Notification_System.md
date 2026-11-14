# 🔔 Question 2: Distributed Notification System

**Subject**: System Design | **Subject Code**: TCS-504  
**Author**: Anubhav Singh | **Institution**: Graphic Era Hill University  
**Faculty Mentor**: Himanshu Namdev  


[![System Design](https://img.shields.io/badge/System%20Design-HLD-orange?style=for-the-badge)](https://github.com/Anubhav-Singh-1234/system-design-hld-assignment)
[![Queues](https://img.shields.io/badge/Queuing-Kafka%20%7C%20RabbitMQ-red?style=for-the-badge)](https://github.com/Anubhav-Singh-1234/system-design-hld-assignment)
[![Channels](https://img.shields.io/badge/Channels-FCM%20%7C%20APNs%20%7C%20Twilio%20%7C%20SendGrid-blueviolet?style=for-the-badge)](https://github.com/Anubhav-Singh-1234/system-design-hld-assignment)

A highly available, resilient, multi-channel notification engine capable of handling millions of transactional, promotional, and scheduled messages with guaranteed delivery, intelligent deduplication, and exponential backoff retry.

---

## 📌 1. Requirements Engineering

### Functional Requirements (FR)
* **FR1 — User Preferences & Registration**: Users can register and configure multi-channel delivery preferences (Email, SMS, Push) and category subscriptions (Order Updates, Reminders, Marketing).
* **FR2 — Multi-Channel Dispatch**: Seamlessly routes notifications through Push (FCM / APNs), SMS (Twilio / AWS SNS), and Email (SendGrid / AWS SES).
* **FR3 — Tiered Notification Priorities**:
  - *High-Priority / Transactional*: OTPs, order state changes, security alerts (delivered $< 3\text{ s}$).
  - *Medium-Priority*: Scheduled delivery reminders, calendar alerts.
  - *Low-Priority*: Marketing campaigns, bulk newsletters.
* **FR4 — Notification History & Lifecycle Tracking**: Persistent audit log recording states: `QUEUED` $\rightarrow$ `SENT` $\rightarrow$ `DELIVERED` $\rightarrow$ `READ` $\rightarrow$ `FAILED`.
* **FR5 — Fault Tolerance & Retry Architecture**: Exponential backoff retry with jitter, Dead Letter Queues (DLQ) for unrecoverable failures, and automatic channel fallback (e.g. Push $\rightarrow$ SMS).

### Non-Functional Requirements (NFR)
* **High Throughput**: Capable of ingesting and dispatching $> 100\text{k}$ notifications per minute.
* **Zero Duplicate Deliveries (Idempotency)**: Ensures users never receive duplicate alerts during retry spikes.
* **Low Latency**: Transactional notifications delivered in $< 3\text{ seconds}$.
* **Compliance & Quiet Hours**: Honors localized user quiet hours (e.g., 10 PM – 7 AM) and regulatory DND lists.

---

## 🏛️ 2. High-Level Architecture

![Distributed Notification System HLD](02_Notification_System_HLD.png)

<details>
<summary><b>View Architecture Source (Mermaid)</b></summary>

```mermaid
graph TD
    subgraph Producers["Event Producers & Sources"]
        ORDER_SVC["Order Management Service"]
        AUTH_SVC["Auth Service / OTP Generator"]
        MARKETING_SVC["Marketing & Campaign Engine"]
    end

    GW["Notification Ingestion Gateway<br/>Validation, Authorization & Ingestion API"]
    ORDER_SVC -->|"gRPC / REST"| GW
    AUTH_SVC -->|"gRPC / REST"| GW
    MARKETING_SVC -->|"gRPC / REST"| GW

    subgraph Ingestion_Pipeline["Policy Engine & Deduplication"]
        RATE["Token Bucket Rate Limiter"]
        DEDUP["Deduplication Filter<br/>Redis TTL Sliding Window"]
        PREF_FILTER["User Preferences & Quiet Hours Filter"]
    end

    GW --> RATE
    RATE --> DEDUP
    DEDUP --> PREF_FILTER

    subgraph Priority_Queues["Multi-Tier Priority Message Queues (Kafka)"]
        Q_HIGH["High-Priority Queue<br/>Transactional OTPs & Critical Alerts"]
        Q_MED["Medium-Priority Queue<br/>Order Tracking & Account Updates"]
        Q_LOW["Low-Priority Queue<br/>Promotions & Newsletters"]
    end

    PREF_FILTER -->|"High Priority"| Q_HIGH
    PREF_FILTER -->|"Medium Priority"| Q_MED
    PREF_FILTER -->|"Low Priority"| Q_LOW

    subgraph Workers["Template Engine & Channel Workers"]
        TPL["Template Rendering Engine<br/>Localization & Variable Substitution"]
        W_PUSH["Push Notification Workers"]
        W_SMS["SMS Dispatch Workers"]
        W_EMAIL["Email Dispatch Workers"]
    end

    Q_HIGH --> TPL
    Q_MED --> TPL
    Q_LOW --> TPL

    TPL --> W_PUSH
    TPL --> W_SMS
    TPL --> W_EMAIL

    subgraph ThirdParty["External Delivery Providers"]
        FCM["Push Providers: Firebase FCM / Apple APNs"]
        TWILIO["SMS Providers: Twilio / AWS SNS / Sinch"]
        SENDGRID["Email Providers: SendGrid / AWS SES / Mailgun"]
    end

    W_PUSH --> FCM
    W_SMS --> TWILIO
    W_EMAIL --> SENDGRID

    subgraph Reliability["Resilience & Fault Handling"]
        RETRY["Retry Engine<br/>Exponential Backoff + Jitter"]
        DLQ["Dead Letter Queue (DLQ)<br/>Poison Pill Quarantine"]
        ALERT["Ops Alerting & PagerDuty Integration"]
    end

    W_PUSH -.->|"On 5xx Failure"| RETRY
    W_SMS -.->|"On 5xx Failure"| RETRY
    W_EMAIL -.->|"On 5xx Failure"| RETRY
    RETRY -->|"Max Retries Exceeded"| DLQ
    DLQ --> ALERT

    subgraph Persistence["Storage & Audit Tier"]
        HIST_DB[("Notification History & Audit Log<br/>Apache Cassandra / DynamoDB")]
        PREF_DB[("User Notification Preferences DB<br/>PostgreSQL")]
    end

    PREF_FILTER -.-> PREF_DB
    W_PUSH --> HIST_DB
    W_SMS --> HIST_DB
    W_EMAIL --> HIST_DB
```
</details>

---

## 🗄️ 3. Database & Audit Log Schemas

### 1. User Notification Preferences (PostgreSQL)
```sql
CREATE TABLE notification_preferences (
    user_id UUID PRIMARY KEY,
    email_enabled BOOLEAN DEFAULT TRUE,
    sms_enabled BOOLEAN DEFAULT TRUE,
    push_enabled BOOLEAN DEFAULT TRUE,
    promotional_enabled BOOLEAN DEFAULT FALSE,
    order_updates_enabled BOOLEAN DEFAULT TRUE,
    quiet_hours_start TIME DEFAULT '22:00:00',
    quiet_hours_end TIME DEFAULT '07:00:00',
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### 2. High-Throughput Notification History (Apache Cassandra CQL)
*Cassandra is chosen for append-heavy write throughput ($100\text{k}+$ writes/sec) with partitioning by `user_id`:*

```sql
CREATE TABLE notification_history (
    user_id UUID,
    notification_id UUID,
    channel VARCHAR,            -- 'EMAIL', 'SMS', 'PUSH'
    category VARCHAR,           -- 'ORDER_STATUS', 'OTP', 'REMINDER'
    status VARCHAR,             -- 'QUEUED', 'SENT', 'DELIVERED', 'FAILED'
    retry_count INT,
    error_message TEXT,
    created_at TIMESTAMP,
    delivered_at TIMESTAMP,
    PRIMARY KEY ((user_id), created_at, notification_id)
) WITH CLUSTERING ORDER BY (created_at DESC);
```

---

## 🔁 4. Retry Architecture & Channel Fallback

### 1. Exponential Backoff with Jitter
For temporary provider downtime or HTTP 429 (Too Many Requests), delays are calculated as:
$$\text{Delay} = \min\left(t_{\max}, t_{\text{base}} \times 2^{\text{attempt}}\right) + \text{random\_jitter}$$

* **Attempt 1**: $1\text{ s} + \text{jitter}$
* **Attempt 2**: $2\text{ s} + \text{jitter}$
* **Attempt 3**: $4\text{ s} + \text{jitter}$
* **Attempt 4**: $8\text{ s} + \text{jitter}$

### 2. Intelligent Channel Fallback Flow
```mermaid
flowchart TD
    A[Transactional Alert: Order Out for Delivery] --> B[Dispatch via Mobile Push FCM]
    B -- Successful Delivery --> C[Log DELIVERED in Cassandra]
    B -- Device Offline / Push Undelivered in 60s --> D[Fallback to SMS via Twilio]
    D -- Successful Delivery --> C
    D -- SMS Fails / Invalid Phone --> E[Route to Dead Letter Queue & Alert Ops]
```
