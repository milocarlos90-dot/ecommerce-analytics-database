# Orders Fact Layer

## Purpose
`fct_orders` represents the canonical order fact table used for analytics and business reporting.

It combines standardized order attributes and financial metrics into a single conformed fact model that serves as the foundation for revenue and customer analytics.

Grain: **1 row per order**

---

## Model

### fct_orders
- Integrates order lifecycle attributes and financial measures
- Links orders to conformed dimensions
- Defines the analytical source of truth for order performance
- Enforces schema contract and data quality validations

Key attributes:
- `order_id`
- `customer_sk`
- `order_created_at`
- `order_created_date`
- lifecycle status flags

Lifecycle flags:
- `is_processing_order`
- `is_shipped_order`
- `is_completed_order`
- `is_cancelled_order`
- `is_returned_order`

Key measures:
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

`fct_orders` represents the **transactional business event** of an order.

It consolidates two complementary concepts:

- **Order Behavior** → lifecycle state of an order
- **Order Value** → financial performance of an order

The model acts as the central fact table within the ecommerce mart layer.

---

## Upstream Dependencies

Built from:

- `int_orders_enriched`
- `int_orders_financials`
- conformed dimensions (`dim_customers`, `dim_date`, etc.)

---

## Downstream Usage

Used to power:

- customer lifetime value metrics
- revenue analytics
- customer revenue retention (cohorts)
- ecommerce KPI dashboards
- operational order reporting

Example derived analytics:
- `customer_lifetime_value`
- `customer_revenue_retention`

---

## Analytics Guidance

Use `fct_orders` when:
- analyzing revenue
- counting orders
- performing customer-level aggregation
- building cohort or retention models

Avoid using this model for:
- product-level analysis
- session or event analysis

Use lower-grain fact tables instead.


# Customers Dimension

## Purpose
`dim_customers` represents the canonical customer dimension used across analytics and reporting.

It provides a single, analytics-ready customer record containing identity, demographic, geographic, and acquisition attributes.

Grain: **1 row per customer**

---

## Model

### dim_customers
- Standardizes customer attributes from intermediate models
- Defines the conformed customer entity
- Enforces data contracts and data quality validations
- Serves as the primary customer join point across marts

Key identifiers:
- `customer_sk`
- `customer_id`
- `email`

Identity attributes:
- `first_name`
- `last_name`
- `full_name`

Demographics:
- `age`
- `age_group`
- `gender`

Location attributes:
- `street_address`
- `city`
- `state`
- `postal_code`
- `country_standardized`

Geographic attributes:
- `latitude`
- `longitude`
- `customer_geography`

Acquisition attributes:
- `traffic_source`

---

## Modeling Strategy

`dim_customers` defines the **conformed customer entity** for the warehouse.

Customer data is:
- standardized
- validated
- enriched for analytical consistency

The model ensures all fact tables reference a shared customer definition.

---

## Upstream Dependencies

Built from:

- `int_customers`

---

## Downstream Usage

Used by:

- `fct_orders`
- customer lifetime value models
- retention & cohort analysis
- marketing attribution analytics
- customer segmentation reporting

---

## Analytics Guidance

Use `dim_customers` when:
- joining customer attributes to fact tables
- performing segmentation analysis
- building customer-level metrics
- analyzing acquisition performance

Avoid using raw or intermediate customer models directly in analytics.