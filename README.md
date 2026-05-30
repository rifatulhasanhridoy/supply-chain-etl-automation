# Automated Enterprise Supply Chain ETL & AI Insights Pipeline

An end-to-end asynchronous data engineering pipeline built in **n8n** that automates the ingestion, routing, and processing of multi-structured supply chain logistics reports. The system automatically handles parallel data streams, updates a **Microsoft SQL Server** relational database, performs analytical deep-dives, and feeds structural insights to an LLM via **OpenRouter** to distribute responsive HTML executive dashboards.

## 📸 Workflow Layout
![n8n Workflow Canvas](canvas.jpg)

## 🚀 Workflow Architecture Overview

The pipeline executes the following stages seamlessly:
1. **Event Trigger:** Monitors an enterprise mail server for automated inbound shipments logs containing binary CSV file attachments.
2. **Conditional Routing (ETL):** Splits incoming files via an n8n `Split Out` node, evaluating metadata through a programmatic `Switch` node to route streams based on file archetypes (`fact_order_line` raw transactions vs. `fact_aggregate_usa` summary reports).
3. **Database Persistence:** Parses raw datasets dynamically and maps them into optimized relational SQL tables utilizing transactional handling rules to guarantee database integrity.
4. **Unified Analytical Aggregation:** Executes advanced relational subqueries and CTEs across database tables to pinpoint structural operational bottlenecks (worst customer fulfillment failure rates, tracking timelines, and product-level delays).
5. **Generative Intelligence Dashboarding:** Feeds structured JSON data vectors directly to a Generative Model via **OpenRouter**, producing a corporate dark-themed HTML dashboard sent over email.

---

## 📨 Output Executive Dashboard
![Generated Corporate Email](dashboard.jpg)

---

## 📊 Database Schema Design

The system maps transactional realities across two primary target data structures within Microsoft SQL Server:

### 1. Raw Transactional Table (`fact_order_line`)
Stores individual line-item fulfillments for deep root-cause diagnostic queries:
* `order_id` (VARCHAR) - Unique Transaction Primary Key
* `order_placement_date` (DATE) - Chronological event timestamp
* `customer_id` / `product_id` (INT) - Dimensional categorical references
* `order_qty` / `delivery_qty` (INT) - Quantitative volume indicators
* `in_full` / `on_time` / `on_time_in_full` (TINYINT) - Binary fulfillment evaluation flags

### 2. Transactional Aggregates Table (`fact_aggregate_usa`)
Maintains macro performance scores for high-level timeline velocity monitoring:
* `order_id` (VARCHAR) - Primary Key
* `customer_id` (INT) - Target client identifier
* `order_placement_date` (DATE) - Calendar date index
* `on_time` / `in_full` / `otif` (TINYINT) - Boolean health flags

---

## 🔍 Core Analytical Engine (SQL)

Rather than feeding uncompressed raw rows to the LLM agent, the pipeline utilizes optimized SQL scripts featuring **Common Table Expressions (CTEs)** and **Unions** to compress historical data into explicit, high-value insight arrays:

```sql
WITH WorstCustomers AS (
    SELECT TOP 3
        'Worst Customer Bottleneck' as insight_type,
        CAST(customer_id AS VARCHAR(50)) as dimension_id,
        COUNT(*) as total_orders,
        ROUND(AVG(CAST(on_time AS FLOAT)) * 100, 2) as on_time_rate,
        ROUND(AVG(CAST(in_full AS FLOAT)) * 100, 2) as in_full_rate,
        ROUND(AVG(CAST(otif AS FLOAT)) * 100, 2) as otif_rate
    FROM fact_aggregate_usa
    GROUP BY customer_id
    ORDER BY otif_rate ASC
),
WorstDate AS (
    SELECT TOP 1
        'Worst Performance Date' as insight_type,
        CONVERT(VARCHAR(50), order_placement_date, 23) as dimension_id,
        COUNT(*) as total_orders,
        ROUND(AVG(CAST(on_time AS FLOAT)) * 100, 2) as on_time_rate,
        ROUND(AVG(CAST(in_full AS FLOAT)) * 100, 2) as in_full_rate,
        ROUND(AVG(CAST(otif AS FLOAT)) * 100, 2) as otif_rate
    FROM fact_aggregate_usa
    GROUP BY order_placement_date
    ORDER BY otif_rate ASC
)
SELECT 
    'Global Summary' as insight_type, NULL as dimension_id, COUNT(*) as total_orders,
    ROUND(AVG(CAST(on_time AS FLOAT)) * 100, 2) as on_time_rate,
    ROUND(AVG(CAST(in_full AS FLOAT)) * 100, 2) as in_full_rate,
    ROUND(AVG(CAST(otif AS FLOAT)) * 100, 2) as otif_rate
FROM fact_aggregate_usa
UNION ALL
SELECT * FROM WorstCustomers
UNION ALL
SELECT * FROM WorstDate;







