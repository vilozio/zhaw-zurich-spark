-- This SQL is used to illistrate a case when we need to run a
-- complex query on regular basis.
--
-- Retain customers who recently registered and have high risk of churning.
-- Here we use some simple rules (they are made up) to identify high risk customers:
-- 1. Total orders >= 2
-- 2. Session duration hours < 2
-- 3. Cancellation rate > 30%
-- 4. Unique products < 3
-- 5. Last order time is within the last 60 seconds

WITH first_time_customers AS (
    SELECT 
        c.customer_id,
        c.name,
        c.created_at as registration_time,
        c.membership_level
    FROM customers c
    WHERE c.created_at >= NOW() - INTERVAL '24 HOURS'
),

customer_orders AS (
    SELECT
        o.customer_id,
        o.order_id,
        o.product,
        o.cost,
        o.created_at as order_time,
        CASE 
            WHEN o.description LIKE '%cancelled%' THEN 1 
            ELSE 0 
        END as is_cancelled
    FROM orders o
    INNER JOIN first_time_customers ftc ON o.customer_id = ftc.customer_id
),

order_aggregates AS (
    SELECT
        co.customer_id,
        COUNT(co.order_id) as total_orders,
        COUNT(DISTINCT co.product) as unique_products,
        AVG(co.cost) as avg_order_value,
        SUM(co.cost) as total_spent,
        SUM(co.is_cancelled) as cancelled_orders,
        MIN(co.order_time) as first_order_time,
        MAX(co.order_time) as last_order_time,
        (EXTRACT(EPOCH FROM MAX(co.order_time)) - EXTRACT(EPOCH FROM MIN(co.order_time))) / 3600.0 as session_duration_hours
    FROM customer_orders co
    GROUP BY co.customer_id
),

evaluate_churn_risk AS (
SELECT
    *,
    ROUND((cancelled_orders::NUMERIC / NULLIF(total_orders, 0)) * 100, 0) as cancellation_rate,
    CASE
        WHEN 
            total_orders >= 2
            AND session_duration_hours < 2
            AND (cancelled_orders::NUMERIC / NULLIF(total_orders, 0)) > 0.3
            AND unique_products < 3
            AND (EXTRACT(EPOCH FROM NOW()) - EXTRACT(EPOCH FROM last_order_time)) < 60
        THEN 1
        ELSE 0
    END as at_risk_customer
FROM order_aggregates
)

SELECT customer_id, at_risk_customer FROM evaluate_churn_risk;
