# Customers Intermediate Layer

## Purpose
This layer standardizes customer data and defines the canonical analytical customer entity used across the warehouse.

It prepares customer records for dimensional modeling and ensures a consistent customer definition for behavioral and transactional analytics.

---

## Models

### int_customers
- Standardizes customer identifiers
- Applies analytics-friendly naming conventions
- Normalizes customer timestamps
- Ensures one canonical customer record
- Prepares customer entity for dimensional modeling
- Grain: **1 row per customer**

Example derived fields:
- `customer_id`
- `customer_created_at`
- `customer_created_date`
- standardized location attributes
- normalized customer metadata

---

## Customer Strategy

The customer entity represents:

- a unique identifiable user of the platform
- the primary entity linking orders, events, and sessions
- the foundation for customer-centric analytics

This model intentionally avoids behavioral or transactional joins to maintain a clean dimensional design.

---

## Downstream Usage

Used to build:

### dim_customers
Supports customer-level analytics including:
- customer lifecycle analysis
- cohort and retention analysis
- customer acquisition tracking
- lifetime value (LTV) modeling
- geographic customer analysis
- marketing attribution at customer level
