# Customer 360 | Segmentation, Personalization & Fraud Detection Platform

> An end-to-end data platform built on Snowflake and dbt that transforms
> raw customer data into actionable intelligence — powering personalized
> marketing, behavioral segmentation, and proactive fraud detection.

---

## The Business Problem

Imagine a retail company with 50,000 customers. Every day:
- Customers browse products, add to cart, abandon, and purchase
- Payments succeed, fail, get disputed
- Marketing emails get sent — some opened, most ignored
- Some customers buy once and never return
- A small number of transactions look suspicious

All of this data exists — but it sits in separate systems that
never talk to each other. The result:

| Problem | Business Impact |
|---|---|
| No unified customer view | Every team works from different numbers |
| Generic marketing | Same email sent to loyal customers and one-time buyers |
| Reactive fraud | Fraud discovered after money is lost |
| No churn signal | At-risk customers not identified until they leave |
| Missed revenue | High-value customers not treated as VIPs |

**This platform solves all five.**

---

## What We Are Building

A single trusted platform that answers three questions:

### Question 1 — Who are our customers?
Build one unified profile per customer combining everything we know
about them — who they are, what they buy, how they browse, how they
respond to marketing, and how they pay.

### Question 2 — How should we treat each customer differently?
Automatically group customers into behavioral segments and tell the
marketing team exactly what to do with each group — what to offer,
which channel to use, and when.

### Question 3 — Which customers and transactions look risky?
Without any labeled fraud data, automatically score every customer
and transaction for suspicious behavior and surface the top risk
cases for the fraud team to review.

---

## Business Outcomes Expected

| Outcome | What It Means |
|---|---|
| **Unified Customer 360** | One trusted profile per customer — no more conflicting numbers across teams |
| **6 Behavioral Segments** | Every customer automatically bucketed into an actionable group |
| **15-25% Campaign Lift** | Personalized messaging drives higher open and conversion rates |
| **Proactive Fraud Flagging** | Top 3% anomalous customers flagged before damage occurs |
| **Daily Fresh Data** | Pipeline runs every night — business always has yesterday's data |
| **Trusted Reporting** | Automated data quality tests catch errors before they reach dashboards |

---

## Data Sources

All data represents a fictional e-commerce / fintech retail company.
Currently using synthetic data generated inside Snowflake.
S3 ingestion via external stages and Snowpipe will be connected in a later phase.

| Source | What It Contains | Rows | Ingestion |
|---|---|---|---|
| **Customers** | Who the customer is — demographics, location, loyalty tier, acquisition channel, signup date | 50K | Batch |
| **Orders** | What they bought — order value, discounts, channel, payment method, order status | 500K | Batch |
| **Order Items** | Which products — line-level product, quantity, price, category per order | 1M | Batch |
| **Transactions** | How they paid — payment method, success/failure, device, location, card details | 750K | Streaming |
| **Clickstream** | How they browse — page views, product views, cart adds, searches, session behavior | 2M | Streaming |
| **Campaigns** | How they respond to marketing — email opens, clicks, conversions, unsubscribes | 200K | Batch |
| **Products** | What we sell — category, brand, price band, margin | 500 | Batch |

**Total: ~4.5M rows across 7 domains**

---

## Platform Architecture

```
┌─────────────────────────────────────────────────────┐
│                    DATA SOURCES                      │
│                                                      │
│  Customers · Orders · Order Items · Products         │
│  Transactions · Clickstream · Campaigns              │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│                    INGESTION                         │
│                                                      │
│  Current  →  Synthetic data generated natively       │
│              inside Snowflake using GENERATOR()      │
│              4.5M rows across 7 domains              │
│                                                      │
│  Planned  →  AWS S3 External Stage + COPY INTO       │
│              Snowpipe for streaming events           │
│              IAM Storage Integration for security    │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│        TRANSFORMATION  (dbt on Snowflake)            │
│                                                      │
│  RAW      →  Exact landed data. No changes.          │
│  STAGE    →  Clean · Deduplicate · Standardize       │
│  CORE     →  Business logic joins + aggregations     │
│  MART     →  Analytics-ready · BI-facing             │
│  FEATURES →  ML-ready · Normalized · Incremental     │
│  ML       →  Model outputs · Scores · Labels         │
└───────────┬───────────────────────┬─────────────────┘
            │                       │
            ▼                       ▼
┌───────────────────┐   ┌───────────────────────────┐
│   BI & REPORTING  │   │       SNOWPARK ML          │
│                   │   │                            │
│  Power BI / Sigma │   │  KMeans  →  Segmentation   │
│  Dashboards       │   │  IsolationForest → Fraud   │
│  CRM Activation   │   │  Scores written back to    │
│  Marketing Feed   │   │  Snowflake ML schema       │
└───────────────────┘   └───────────────────────────┘
            │                       │
            └───────────┬───────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│         ORCHESTRATION  (Snowflake Tasks)             │
│                                                      │
│  Git Fetch → Ingest → Stage → Test → Mart →          │
│  Features  → Test  → ML Score → Snapshot → Audit    │
│                                                      │
│  Runs daily at 02:00 UTC · Fully native Snowflake    │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│            CI / CD  (GitHub Actions)                 │
│                                                      │
│  Pull Request  →  dbt compile + parse                │
│                   Catches syntax errors before merge │
│                                                      │
│  PR to main    →  dbt run + dbt test                 │
│                   Runs on temp CI schema in          │
│                   Snowflake · Auto-deleted after     │
│                                                      │
│  Merge to main →  dbt docs generate                  │
│                   Auto-deploys to GitHub Pages       │
└─────────────────────────────────────────────────────┘
```
---

## Medallion Architecture — Layer by Layer

### RAW
Exact data as it lands from S3. No transformations.
Append-only. Every row has _LOADED_AT timestamp for freshness tracking.

### STAGE
Cleaned and standardized raw data. One model per source table.
- Deduplicate records
- Standardize text (lowercase, trim, proper case)
- Cast data types correctly
- Derive simple flags and calculated fields
- Filter out completely invalid rows

### CORE
Business logic joins and aggregations. Never queried directly.
Feeds into the mart layer. Computed once, reused by many models.

### MART
Analytics-ready tables used by BI tools and business teams.

**Dimensions:**
- dim_customers — one row per customer with full profile

**Facts:**
- fct_orders — one row per order
- fct_transactions — one row per payment transaction
- fct_campaigns — one row per campaign response
- fct_customer_value — one row per customer combining all domains

**Metrics calculated in the mart layer:**

| Metric | Description |
|---|---|
| recency_days | Days since last order |
| total_orders | Lifetime order count |
| total_revenue | Lifetime spend |
| avg_order_value | Average spend per order |
| orders_last_90d | Orders in last 90 days |
| revenue_last_90d | Revenue in last 90 days |
| discount_ratio | Proportion of orders using a discount |
| cart_abandon_rate | Sessions with cart add but no purchase |
| sessions_last_30d | Active sessions in last 30 days |
| email_open_rate | Proportion of campaigns opened |
| email_click_rate | Proportion of campaigns clicked |
| failed_txn_rate | Proportion of payments that failed |
| chargeback_rate | Proportion of payments disputed |
| unique_cards_30d | Distinct cards used in 30 days |
| unique_devices_30d | Distinct devices used in 30 days |
| night_activity_ratio | Transactions between midnight and 6am |
| customer_value_score | Composite 0-100 score: recency + frequency + monetary + engagement |
| churn_risk_score | Composite 0-100 score: declining activity + discount dependency |
| personalization_score | Composite 0-100 score: data richness for personalization |

### FEATURES
ML-ready feature tables. Normalized to 0-1 range for KMeans compatibility.
Incremental materialization — only new/changed customers processed daily.

- feat_customer_segmentation — 10 normalized behavioral features per customer
- feat_fraud_behavior — device risk, card velocity, geo velocity, rapid transaction signals
- feat_personalization — category affinity, channel preference, time-of-day preference

### ML
Model outputs written back to Snowflake after Snowpark ML scoring.

- customer_segments — cluster assignment + segment name + recommended action
- customer_risk_scores — anomaly score + risk tier (HIGH / MEDIUM / LOW / NORMAL)

---

## Customer Segments

| Segment | Who They Are | What We Do |
|---|---|---|
| High-Value Loyalists | High spend, frequent buyers, low churn risk | Early access, loyalty rewards, premium bundles |
| Deal Seekers | Price-sensitive, promo-driven, discount-dependent | Flash sales, coupons, price-drop alerts |
| New and Promising | Recent signup, early purchases, growing spend | Onboarding journey, first-purchase incentive |
| Browsers | High sessions, low conversion, high cart abandon | Retargeting, social proof, urgency messaging |
| At-Risk Churning | Declining recency, dropping frequency | Win-back campaign, satisfaction survey |
| Premium Niche | Low order count, very high AOV, low discount use | Curated recommendations, exclusive previews |

---

## Data Quality Framework

| Level | Tool | What It Checks |
|---|---|---|
| Source freshness | src_all.yml | Is raw data arriving on time? |
| Source tests | src_all.yml | Are primary keys unique and not null in raw? |
| Staging tests | staging.yml | Are cleaned columns valid after transformation? |
| Relationship tests | staging.yml | Do foreign keys exist in parent tables? |
| Mart tests | schema.yml | Are business metrics within expected ranges? |
| Singular tests | tests/ folder | Custom SQL business rules |
| Pipeline audit | MONITORING.PIPELINE_AUDIT | Full run history with row counts and errors |

---

## Snowflake Infrastructure

| Object Type | Count | Details |
|---|---|---|
| Database | 1 | CUSTOMER_PLATFORM |
| Schemas | 7 | RAW, STAGE, CORE, MART, FEATURES, ML, MONITORING |
| Warehouses | 4 | WH_INGEST, WH_DBT_TRANSFORM, WH_ML, WH_ADHOC |
| Roles | 5 | INGEST_ROLE, TRANSFORMER_ROLE, ML_ROLE, ANALYST_ROLE, MONITOR_ROLE |
| Raw Tables | 8 | All source tables + PIPELINE_AUDIT |

---

## Project Progress

### Phase 1 — Snowflake Foundation ✅
- Database, schemas, warehouses, roles, grants
- All 8 raw table DDLs including pipeline audit log
- 4.5M rows synthetic data across 7 domains
- Snowflake connected to GitHub via Git Repository integration

### Phase 2 — dbt Staging Layer ✅
- dbt project initialized
- All 7 source tables declared with freshness SLAs
- 7 staging models built and tested
- Derived fields: tenure_band, age_band, loyalty_tier_rank,
  net_revenue, fraud flags, identity resolution,
  engagement_level, margin_pct, time_of_day
- staging.yml with full tests and documentation

### Phase 3 — Intermediate Layer ✅
- int_customer_orders — RFM aggregations per customer
- int_customer_transactions — payment volume and rates
- int_customer_fraud_signals — device, geo, rapid transaction risk
- int_session_features — behavioral aggregations
- int_product_affinity — category affinity scores

### Phase 4 — Mart Layer ✅
- dim_customers, fct_orders, fct_transactions
- fct_campaigns, fct_customer_value (Customer 360)
- 74 tests passing across all mart models

### Phase 5 — Feature Layer ✅
- feat_customer_segmentation — normalized ML features
- feat_fraud_behavior — fraud signal features
- feat_personalization — personalization + recommended actions

### Phase 6 — Supporting Files ✅
- Macros: generate_schema_name, dq_helpers
- Seeds: segment_thresholds
- Snapshots: snap_customer_profile (SCD Type 2)
- Singular tests: 4 custom SQL business rule tests
- Exposures: 7 downstream consumers declared
- packages.yml: dbt_utils + dbt_expectations

### Phase 7 — CI/CD ✅
- GitHub Actions: dbt compile on PR
- GitHub Actions: dbt build + test on Snowflake CI schema
- GitHub Actions: dbt docs deploy to GitHub Pages

### Phase 8 — Snowpark ML ⬜ Next
- KMeans customer segmentation
- IsolationForest anomaly detection

### Phase 9 — Orchestration ⬜ Upcoming
- Snowflake Task chain (10 steps)
- Daily automated pipeline at 02:00 UTC

### Phase 10 — S3 Ingestion ⬜ Later
- External stages + COPY INTO
- Snowpipe for streaming

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Warehouse | Snowflake |
| Transformation | dbt Core |
| Orchestration | Snowflake Tasks |
| ML | Snowpark ML (KMeans + IsolationForest) |
| Version Control | GitHub + Snowflake Git Integration |
| Ingestion | Snowflake External Stage + COPY INTO + Snowpipe |
| Raw Storage | AWS S3 |
| BI | Power BI / Sigma |

---

## Author

**Sriram S** | Analytics Engineering + Data Science
MS Business Analytics and Information Systems — University of South Florida
AWS ML Engineer Certified | Google Advanced Data Analytics Certified.
