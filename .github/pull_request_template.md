<!-- ===================================================== -->
<!-- Pull Request Template                                 -->
<!-- ===================================================== -->

<!---
Good PR title examples:
* Feature: add intermediate models
* Fix: deduplicate customer records
* Update: refactor transformation logic
-->

## Description & Motivation
Briefly describe:
- What changed
- Why it was needed
- Related ticket or issue

---

## Reviewer Guide
Suggested review order:

1. Review DAG screenshot.
2. Review model SQL logic and grain.
3. Review schema.yml tests & documentation.
4. Review validation checks below.

---

## Model Grain
Declare grain for models introduced or modified.

| Model | Grain |
|------|------|
| MODEL_NAME | DEFINE GRAIN |

---

## DAG Screenshot
Run locally:

dbt docs generate && dbt docs serve

Attach updated DAG screenshot.

---

## Validation

### dbt Validation
- [ ] `dbt build` completed successfully.
- [ ] All dbt tests passed (`unique`, `not_null`, `relationships`, `accepted_values`).

### Data Validation Checks
- [ ] Model grain validated.
- [ ] Row counts validated before and after transformations.
- [ ] No join fan-out introduced.
- [ ] Null drift checked for enriched or derived columns.
- [ ] Business logic validated where applicable (e.g. deduplication).

---

## Checklist

### Scope & Version Control
- [ ] PR represents one logical change.
- [ ] Commits are clean and readable.

### Modeling & Implementation
- [ ] Models materialized appropriately.
- [ ] Tests added or updated.
- [ ] Documentation added or updated.

### Documentation
- [ ] README updated where required.

---

## Validation SQL (BigQuery)

Replace placeholders before running:

PROJECT_ID  
DEV_DATASET  
MODEL_TABLE  
PRIMARY_KEY  
BUSINESS_KEY  
ORDER_COLUMN  
UPSTREAM_DATASET  
UPSTREAM_TABLE  
COLUMN_NAME  

```sql
-- =====================================================
-- GRAIN VALIDATION
-- =====================================================
select
  count(*) as total_rows,
  count(distinct PRIMARY_KEY) as distinct_keys
from `PROJECT_ID.DEV_DATASET.MODEL_TABLE`;


-- =====================================================
-- JOIN FAN-OUT DETECTION
-- =====================================================
select
  count(*) as total_rows,
  count(distinct PRIMARY_KEY) as distinct_keys,
  count(*) - count(distinct PRIMARY_KEY) as duplicate_rows
from `PROJECT_ID.DEV_DATASET.MODEL_TABLE`;


-- =====================================================
-- ROW COUNT COMPARISON
-- =====================================================
select
  (select count(*)
   from `PROJECT_ID.UPSTREAM_DATASET.UPSTREAM_TABLE`) as upstream_rows,
  (select count(*)
   from `PROJECT_ID.DEV_DATASET.MODEL_TABLE`) as model_rows;


-- =====================================================
-- NULL DRIFT CHECK
-- =====================================================
select
    'upstream' as layer,
    countif(COLUMN_NAME is null) as null_count,
    count(*) as total_rows,
    safe_divide(countif(COLUMN_NAME is null), count(*)) as null_rate
from `PROJECT_ID.UPSTREAM_DATASET.UPSTREAM_TABLE`

union all

select
    'model',
    countif(COLUMN_NAME is null),
    count(*),
    safe_divide(countif(COLUMN_NAME is null), count(*))
from `PROJECT_ID.DEV_DATASET.MODEL_TABLE`;


-- =====================================================
-- DUPLICATE INVESTIGATION TEMPLATE
-- =====================================================
select
    BUSINESS_KEY,
    *,
    row_number() over (
        partition by BUSINESS_KEY
        order by ORDER_COLUMN desc
    ) as dedupe_rank,
    count(*) over (
        partition by BUSINESS_KEY
    ) as duplicate_count
from `PROJECT_ID.DEV_DATASET.MODEL_TABLE`
where BUSINESS_KEY in (
    select BUSINESS_KEY
    from `PROJECT_ID.DEV_DATASET.MODEL_TABLE`
    group by BUSINESS_KEY
    having count(*) > 1
)
order by BUSINESS_KEY, dedupe_rank;