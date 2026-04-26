# Customer 360 | Segmentation, Personalization & Fraud Detection Platform
### Snowflake · dbt Cloud · MetricFlow · Snowpark ML · GitHub Actions

[![dbt](https://img.shields.io/badge/dbt-FF694A?style=flat&logo=dbt&logoColor=white)](https://cloud.getdbt.com)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)](https://snowflake.com)
[![GitHub Actions](https://img.shields.io/badge/CI%2FCD-2088FF?style=flat&logo=github-actions&logoColor=white)](/.github/workflows)
[![Tests](https://img.shields.io/badge/dbt_tests-290_passing-3FB950?style=flat)](#data-quality)
[![Live](https://img.shields.io/badge/dashboard-live-3FB950?style=flat)](https://sriramsripada20s.github.io/customer-360-snowflake-dbt)

**[→ Live Portfolio Dashboard](https://sriramsripada20s.github.io/customer-360-snowflake-dbt)** — real production metrics auto-updating daily from Snowflake

---

## The Business Problem

Imagine a fintech neobank with 50,000 customers. Every day:
- Customers browse products, add to cart, abandon, and purchase
- Payments succeed, fail, get disputed
- Marketing emails get sent — some opened, most ignored
- Some customers buy once and never return
- A small number of transactions look suspicious

All of this data exists — but it sits in separate systems that never talk to each other. The result:

| Problem | Business Impact |
|---|---|
| No unified customer view | Every team works from different numbers |
| Generic marketing | Same email sent to loyal customers and one-time buyers |
| Reactive fraud | Fraud discovered after money is already lost |
| No churn signal | At-risk customers not identified until they leave |
| Missed revenue | High-value customers not treated differently |

**This platform solves all five.**

---

## What We Built

A single trusted platform that answers three questions:

### Question 1 — Who are our customers?
One unified profile per customer combining everything we know about them — who they are, what they buy, how they browse, how they respond to marketing, and how they pay.

### Question 2 — How should we treat each customer differently?
Automatically segment customers by behavior and tell the marketing team exactly what to do with each group — what to offer, which channel to use, and when.

### Question 3 — Which customers and transactions look risky?
Automatically score every customer and transaction for suspicious behavior and surface the top risk cases for the fraud team before money is lost.

---

## Business Outcomes

| Outcome | What It Means |
|---|---|
| **Unified Customer 360** | One trusted profile per customer — no more conflicting numbers |
| **Behavioral Segmentation** | Every customer automatically bucketed into an actionable group |
| **Proactive Fraud Flagging** | Anomalous customers flagged before damage occurs |
| **18 Canonical Metrics** | Single source of truth for churn rate, revenue, LTV across all BI tools |
| **Daily Fresh Data** | Pipeline runs every morning — business always has today's data |
| **290 Automated Tests** | Data quality enforced at every layer before reaching dashboards |

---

## Platform Metrics

| Metric | Value |
|---|---|
| Customers | 50,000 |
| Active customers | 47,500 (95%) |
| Transactions processed | 750K+ |
| Total revenue tracked | $224M |
| dbt models | 15 |
| Automated tests | 290 passing |
| MetricFlow metrics | 18 |
| Churn rate | 5.0% |
| Avg order value | $250 |
| Daily prod refresh | 6 AM UTC |

---



---

## Architecture

<img width="1148" height="843" alt="image" src="https://github.com/user-attachments/assets/75e0206f-da9d-4aed-8e63-ac77979e2d7b" />


## Medallion Architecture — Layer by Layer

View the Entire data sources & their definition here: https://sriramsripada20s.github.io/customer-360-snowflake-dbt/customer_360_registry.html


### RAW


### STAGING
Cleaned and standardized raw data. One model per source table.
- Deduplicate records using `QUALIFY ROW_NUMBER()`
- Standardize text — lowercase, trim, proper case
- Cast data types correctly
- Derive simple flags — `is_active`, `is_returned`, `has_promo`
- Filter completely invalid rows

### INTERMEDIATE
Business logic joins and aggregations. Ephemeral — never queried directly. Computed once, reused by many downstream models.

### MART
Analytics-ready tables used by BI tools and business teams.

**Dimensions:** `dim_customers` — one row per customer with full profile

**Facts:**
- `fct_orders` — one row per order
- `fct_customer_value` — one row per customer combining all domains

**Composite scores in `fct_customer_value`:**

| Score | Formula | Range |
|---|---|---|
| `customer_value_score` | Recency 25pts + Frequency 25pts + Monetary 30pts + Engagement 20pts | 0–100 |
| `churn_risk_score` | Recency decline + Frequency drop + Discount dependency + Payment failures | 0–100 |
| `personalization_score` | Data richness across categories, channels, time-of-day preferences | 0–100 |

### FEATURES
ML-ready feature tables. Normalized to 0–1 range for KMeans compatibility. Incremental materialization — only new/changed customers processed daily.

- `feat_customer_segmentation` — 10 normalized behavioral features
- `feat_fraud_behavior` — device risk, card velocity, geo velocity, rapid transaction signals
- `feat_personalization` — category affinity, channel preference, time-of-day preference

---

## Customer Segments

| Segment | Who They Are | Recommended Action |
|---|---|---|
| High-Value Loyalists | High spend, frequent buyers, low churn risk | Early access, loyalty rewards, premium bundles |
| Deal Seekers | Price-sensitive, promo-driven, discount-dependent | Flash sales, coupons, price-drop alerts |
| New and Promising | Recent signup, early purchases, growing spend | Onboarding journey, first-purchase incentive |
| Browsers | High sessions, low conversion, high cart abandon | Retargeting, social proof, urgency messaging |
| At-Risk Churning | Declining recency, dropping frequency | Win-back campaign, satisfaction survey |
| Premium Niche | Low order count, very high AOV, low discount use | Curated recommendations, exclusive previews |

---

## MetricFlow Semantic Layer

18 canonical business metrics defined once in YAML — queryable from Sigma, Power BI, and the dbt Cloud API with consistent definitions. No more metric drift between teams.

**Customer:** `total_customers` · `active_customers` · `avg_recency_days`

**Revenue:** `total_revenue` · `revenue_last_30d` · `revenue_last_90d` · `avg_order_value` · `revenue_per_active_customer` · `avg_customer_value_score`

**Risk:** `churn_rate` · `avg_churn_risk_score` · `high_risk_customer_count` · `avg_failed_txn_rate` · `avg_chargeback_rate` · `avg_cart_abandon_rate`

**Engagement:** `avg_email_open_rate` · `avg_email_click_rate` · `avg_personalization_score`

**All metrics sliceable by:** `loyalty_tier` · `age_band` · `acquisition_channel` · `region` · `is_active`

```bash
dbt sl query --metrics churn_rate --group-by customer__loyalty_tier
dbt sl query --metrics total_revenue,active_customers --group-by customer__age_band
dbt sl query --metrics avg_churn_risk_score --group-by customer__acquisition_channel
```

---

## Data Quality

290 automated tests run on every `dbt build` — no model reaches production without passing all tests.

| Test | What It Checks |
|---|---|
| `not_null` | All primary keys and critical columns |
| `unique` | All primary keys across every model |
| `accepted_values` | Status fields, loyalty tiers, payment methods |
| `relationships` | FK integrity across all dims and facts |
| `assert_no_duplicate_customers` | Deduplication validation |
| `assert_no_negative_revenue` | Revenue sanity check |
| `assert_valid_segment_labels` | Segment consistency |
| `assert_feature_norm_bounds` | ML feature range validation |
| Source freshness SLAs | Alerts if source tables go stale |

---

## dbt Cloud Setup

### Environments

| Environment | Snowflake Schema | Purpose |
|---|---|---|
| Dev | `CUSTOMER_PLATFORM.DBT_SRIRAM` | Development and testing |
| Production | `CUSTOMER_PLATFORM.DBT_SRIRAM_PROD` | Scheduled daily builds |

### Jobs

| Job | Schedule | Commands |
|---|---|---|
| Daily Pipeline Refresh | 6 AM UTC | `dbt build` + `dbt docs generate` |
| CI Validation | Every PR | `dbt compile` + `dbt build` |

---

## CI/CD

Every pull request to `main` triggers:

```
1. dbt deps      → install packages
2. dbt compile   → catch syntax + ref errors (no Snowflake needed)
3. dbt build     → run all 15 models + all 290 tests on CI schema
4. Block merge   → if any test fails
```

After merge — production job at 6 AM UTC via dbt Cloud:

```
1. Clone latest main
2. dbt build     → rebuild all models in DBT_SRIRAM_PROD
3. dbt docs generate → update lineage DAG
4. export_metrics.py → query Snowflake once → write metrics.json
5. GitHub Pages  → serve updated live dashboard
```

---

## Live Portfolio Dashboard

The dashboard reads from `docs/metrics.json` — a static file updated once per day. Zero live Snowflake hits on page load.

```
6:00 AM UTC  — dbt Cloud runs dbt build (prod job)
6:30 AM UTC  — GitHub Actions runs export_metrics.py
               Queries Snowflake once for all 18 metrics
               Writes docs/metrics.json
               Commits to main
               GitHub Pages serves updated numbers instantly
```

**[sriramsripada20s.github.io/customer-360-snowflake-dbt](https://sriramsripada20s.github.io/customer-360-snowflake-dbt)**

---

## Project Structure

```
customer-360-snowflake-dbt/
├── .github/
│   └── workflows/
│       ├── dbt_ci.yml              # CI — dbt build on every PR
│       └── update_metrics.yml      # exports metrics daily
├── customer_360/                   # dbt project
│   ├── models/
│   │   ├── sources/                # source definitions + freshness SLAs
│   │   ├── staging/                # 7 staging models (views)
│   │   ├── intermediate/           # 5 ephemeral models
│   │   ├── mart/                   # dim_customers, fct_orders, fct_customer_value
│   │   ├── features/               # ML-ready feature tables
│   │   └── metrics/                # MetricFlow semantic layer YAML
│   ├── macros/                     # reusable SQL macros
│   ├── tests/                      # 4 custom singular tests
│   ├── snapshots/                  # snap_customer_profile (SCD Type 2)
│   ├── seeds/                      # segment_thresholds
│   ├── dbt_project.yml
│   └── packages.yml
├── docs/
│   ├── index.html                  # live portfolio dashboard
│   └── metrics.json                # auto-updated daily from Snowflake
├── export_metrics.py               # queries Snowflake → writes metrics.json
└── README.md
```

---

## Snowflake Infrastructure

| Object | Detail |
|---|---|
| Database | `CUSTOMER_PLATFORM` |
| Dev schema | `DBT_SRIRAM` |
| Prod schema | `DBT_SRIRAM_PROD` |
| Warehouse | `COMPUTE_WH` |
| Role | `ACCOUNTADMIN` |
| Raw tables | 7 source tables |
| Prod tables | 3 mart + 3 feature tables |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data warehouse | Snowflake |
| Transformation | dbt Cloud |
| Semantic layer | MetricFlow |
| Orchestration | dbt Cloud Jobs |
| CI/CD | GitHub Actions |
| ML features | Snowpark Python |
| BI | Sigma Computing · Power BI |
| Portfolio dashboard | GitHub Pages + Python |
| Version control | GitHub |

---

## What's Next

| Item | Status |
|---|---|
| Self-healing diagnostic agent (Claude API + ReAct) | 🔄 In progress |
| Streamlit demo for Cortex AI Agent | ⬜ Planned |
| dbt data contracts on dim_customers | ⬜ Planned |
| Snowflake Dynamic Tables experiment | ⬜ Planned |

---

## Author

**Sriram Sripada** — Analytics Engineer · Data Scientist  
MS Business Analytics & Information Systems — University of South Florida  
AWS ML Certified · Google Advanced Data Analytics · Stanford ML Specialization · dbt Fundamentals
