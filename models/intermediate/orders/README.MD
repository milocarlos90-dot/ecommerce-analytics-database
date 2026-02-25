# Orders Intermediate Layer

## Purpose
This layer standardizes and enriches order data to create analytics-ready order entities and financial metrics.

It transforms transactional order data into consistent business objects used for downstream reporting and dimensional modeling.

---

## Models

### int_orders_enriched
- Renames analytical fields
- Standardizes order timestamps
- Creates lifecycle boolean flags
- Defines order business semantics
- Grain: **1 row per order**

Example derived fields:
- `order_created_at`
- `order_created_date`
- `is_completed_order`
- `is_cancelled_order`
- `is_shipped_order`
- `is_returned_order`
- `is_processing_order`

---

### int_orders_financials
- Aggregates order item data to order grain
- Calculates order-level revenue metrics
- Computes item and product counts
- Separates revenue by lifecycle status
- Grain: **1 row per order**

Example metrics:
- `gross_revenue`
- `completed_revenue`
- `cancelled_revenue`
- `returned_revenue`
- `shipped_revenue`
- `processing_revenue`
- `item_count`
- `distinct_products`

---

## Modeling Strategy

Two complementary order concepts exist:

- **Order Attributes** → lifecycle & status behavior (`int_orders_enriched`)
- **Order Financials** → monetary aggregation (`int_orders_financials`)

---

## Downstream Usage

Used to build:

- `fct_orders`
- revenue analytics
- customer lifetime value models
- retention & cohort analysis
- operational order reporting
