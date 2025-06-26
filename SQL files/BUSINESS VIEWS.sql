USE SCHEMA customer360.gold_layer;

-- ========================================================
-- 1. CUSTOMER_LIFETIME_VALUE
-- ========================================================
CREATE OR REPLACE VIEW CUSTOMER360.GOLD_LAYER.CUSTOMER_LIFETIME_VALUE AS
SELECT 
    dc.customer_id,
    dc.full_name,
    dc.email,
    SUM(ft.amount) AS customer_lifetime_value,
    MIN(dd.dates) AS first_transaction_date,
    MAX(dd.dates) AS last_transaction_date,
    DATEDIFF(DAY, MIN(dd.dates), MAX(dd.dates)) AS transaction_period_days
FROM customer360.gold_layer.fact_transactions ft
JOIN customer360.gold_layer.dim_customers dc 
    ON ft.customer_id_sk = dc.customer_sk
JOIN customer360.gold_layer.dim_date dd 
    ON ft.transaction_date = dd.date_id_sk
GROUP BY 
    dc.customer_id, 
    dc.full_name, 
    dc.email
ORDER BY customer_lifetime_value DESC;

-- ========================================================
-- 2. CUSTOMER_ACTIVITY_STATUS
-- ========================================================
CREATE OR REPLACE VIEW CUSTOMER360.GOLD_LAYER.CUSTOMER_ACTIVITY_STATUS AS
WITH last_transaction AS (
    SELECT 
        ft.customer_id_sk,
        MAX(dd.dates) AS last_transaction_date
    FROM customer360.gold_layer.fact_transactions ft
    JOIN customer360.gold_layer.dim_date dd 
        ON ft.transaction_date = dd.date_id_sk
    GROUP BY ft.customer_id_sk
),
categorized_users AS (
    SELECT 
        dc.customer_id,
        dc.full_name,
        dc.email,
        dc.city,
        dc.state_name,
        lt.last_transaction_date,
        CASE 
            WHEN lt.last_transaction_date IS NULL THEN 'Inactive'
            WHEN lt.last_transaction_date >= DATEADD(MONTH, -3, (SELECT MAX(dates) FROM CUSTOMER360.GOLD_LAYER.dim_date)) THEN 'Active'
            ELSE 'Dormant'
        END AS user_category
    FROM customer360.gold_layer.dim_customers dc
    LEFT JOIN last_transaction lt 
        ON dc.customer_sk = lt.customer_id_sk
)
SELECT 
    customer_id,
    full_name,
    email,
    city,
    state_name,
    last_transaction_date,
    user_category
FROM categorized_users
ORDER BY user_category, customer_id;

-- ========================================================
-- 3. SIGNUPS_OVER_TIME
-- ========================================================
CREATE OR REPLACE VIEW CUSTOMER360.GOLD_LAYER.SIGNUPS_OVER_TIME AS
SELECT 
    dd.years,
    dd.months,
    dd.month_name,
    COUNT(dc.customer_id) AS signup_count
FROM CUSTOMER360.GOLD_LAYER.dim_customers dc
JOIN CUSTOMER360.GOLD_LAYER.dim_date dd 
    ON dc.signup_date_sk = dd.date_id_sk
GROUP BY dd.years, dd.months, dd.month_name
ORDER BY dd.years, dd.months;

-- ========================================================
-- 4. MONTHLY_CHURN_AND_RETENTION
-- ========================================================
CREATE OR REPLACE VIEW CUSTOMER360.GOLD_LAYER.MONTHLY_CHURN_AND_RETENTION AS
WITH customer_periods AS (
    SELECT 
        ft.customer_id_sk,
        DATE_TRUNC('month', dd.dates) AS activity_month,
        MIN(dd.dates) OVER (PARTITION BY ft.customer_id_sk) AS first_transaction_date
    FROM customer360.gold_layer.fact_transactions ft
    JOIN customer360.gold_layer.dim_date dd 
        ON ft.transaction_date = dd.date_id_sk
),
monthly_retention AS (
    SELECT 
        activity_month,
        COUNT(DISTINCT customer_id_sk) AS customers_at_end,
        COUNT(DISTINCT CASE 
            WHEN DATE_TRUNC('month', first_transaction_date) < activity_month THEN customer_id_sk 
        END) AS returning_customers,
        COUNT(DISTINCT CASE 
            WHEN DATE_TRUNC('month', first_transaction_date) = activity_month THEN customer_id_sk 
        END) AS new_customers
    FROM customer_periods
    GROUP BY activity_month
),
final_metrics AS (
    SELECT 
        activity_month,
        TO_CHAR(activity_month, 'YYYY-MM') AS period,
        TO_CHAR(activity_month, 'Mon') AS month_name,
        EXTRACT(YEAR FROM activity_month) AS year,
        EXTRACT(MONTH FROM activity_month) AS month_number,
        LAG(customers_at_end) OVER (ORDER BY activity_month) AS customers_at_start,
        customers_at_end,
        new_customers,
        customers_at_end - new_customers AS retained_customers
    FROM monthly_retention
),
with_rates AS (
    SELECT 
        *,
        ROUND((retained_customers * 100.0) / NULLIF(customers_at_start, 0), 2) AS retention_rate_percent,
        customers_at_start - retained_customers AS churned_customers,
        ROUND(((customers_at_start - retained_customers) * 100.0) / NULLIF(customers_at_start, 0), 2) AS churn_rate_percentage
    FROM final_metrics
)
SELECT *
FROM with_rates
WHERE customers_at_start IS NOT NULL
ORDER BY activity_month;

-- ========================================================
-- 5. CHURN_RATE_BY_LOCATION
-- ========================================================
CREATE OR REPLACE VIEW CUSTOMER360.GOLD_LAYER.CHURN_RATE_BY_LOCATION AS
WITH active_customers AS (
    SELECT 
        dc.customer_id,
        dc.city,
        dc.state_name,
        MAX(dd.dates) AS last_activity_date
    FROM dim_customers dc
    LEFT JOIN fact_transactions ft 
        ON dc.customer_sk = ft.customer_id_sk
    LEFT JOIN dim_date dd 
        ON ft.transaction_date = dd.date_id_sk
    GROUP BY dc.customer_id, dc.city, dc.state_name
),
current_period AS (
    SELECT 
        DATE_TRUNC('MONTH', MAX(dates)) AS current_month_end,
        DATEADD(MONTH, -1, DATE_TRUNC('MONTH', MAX(dates))) AS previous_month_end
    FROM dim_date
),
churned_customers AS (
    SELECT
        ac.city,
        ac.state_name,
        COUNT(DISTINCT ac.customer_id) AS churned_count,
        COUNT(*) OVER () AS total_churned
    FROM active_customers ac
    CROSS JOIN current_period cp
    WHERE 
        ac.last_activity_date BETWEEN DATEADD(MONTH, -2, cp.previous_month_end) 
                                  AND cp.previous_month_end
        AND ac.last_activity_date < DATEADD(MONTH, -1, cp.current_month_end)
    GROUP BY ac.city, ac.state_name
),
total_customers AS (
    SELECT
        city,
        state_name,
        COUNT(DISTINCT customer_id) AS total_customers
    FROM active_customers
    GROUP BY city, state_name
)
SELECT
    tc.city,
    tc.state_name AS state,
    tc.total_customers,
    COALESCE(cc.churned_count, 0) AS churned_customers,
    CASE 
        WHEN tc.total_customers > 0 
        THEN ROUND((COALESCE(cc.churned_count, 0) * 100.0) / tc.total_customers, 2)
        ELSE 0 
    END AS churn_rate_percentage,
    ROUND(COALESCE(cc.churned_count, 0) * 100.0 / NULLIF(cc.total_churned, 0), 2) AS location_share_of_total_churn
FROM total_customers tc
LEFT JOIN churned_customers cc 
    ON tc.city = cc.city AND tc.state_name = cc.state_name
ORDER BY churn_rate_percentage DESC;

-- ========================================================
-- 6. CHURNED_CUSTOMERS_BY_LOCATION
-- ========================================================
CREATE OR REPLACE VIEW CUSTOMER360.GOLD_LAYER.CHURNED_CUSTOMERS_BY_LOCATION AS
WITH customer_activity AS (
    SELECT
        dc.customer_id,
        dc.full_name,
        dc.email,
        dc.phone,
        dc.city,
        dc.state_name,
        signup_date.dates AS signup_date,
        MAX(ft_dates.dates) AS last_transaction_date,
        MAX(sess_dates.dates) AS last_session_date,
        MAX(sup_dates.dates) AS last_support_date
    FROM dim_customers dc
    JOIN dim_date signup_date 
        ON dc.signup_date_sk = signup_date.date_id_sk
    LEFT JOIN fact_transactions ft 
        ON dc.customer_sk = ft.customer_id_sk
    LEFT JOIN dim_date ft_dates 
        ON ft.transaction_date = ft_dates.date_id_sk
    LEFT JOIN dim_sessions sess 
        ON dc.customer_sk = sess.customer_id_sk
    LEFT JOIN dim_date sess_dates 
        ON sess.interaction_date_sk = sess_dates.date_id_sk
    LEFT JOIN dim_support sup 
        ON dc.customer_sk = sup.customer_id_sk
    LEFT JOIN dim_date sup_dates 
        ON sup.created_date_sk = sup_dates.date_id_sk
    GROUP BY 
        dc.customer_id, dc.full_name, dc.email, dc.phone, 
        dc.city, dc.state_name, signup_date.dates
),
activity_summary AS (
    SELECT
        ca.*,
        GREATEST(
            COALESCE(last_transaction_date, signup_date),
            COALESCE(last_session_date, signup_date),
            COALESCE(last_support_date, signup_date),
            signup_date
        ) AS last_activity_date
    FROM customer_activity ca
),
current_reference AS (
    SELECT MAX(dates) AS current_date FROM dim_date
),
churn_identification AS (
    SELECT
        a.customer_id,
        a.full_name,
        a.email,
        a.phone,
        a.city,
        a.state_name,
        a.signup_date,
        a.last_activity_date,
        a.last_transaction_date,
        a.last_session_date,
        a.last_support_date,
        cr.current_date,
        DATEDIFF(DAY, a.last_activity_date, cr.current_date) AS days_inactive,
        CASE WHEN a.last_transaction_date IS NOT NULL THEN 'Yes' ELSE 'No' END AS had_transactions,
        CASE WHEN a.last_session_date IS NOT NULL THEN 'Yes' ELSE 'No' END AS had_sessions,
        CASE WHEN a.last_support_date IS NOT NULL THEN 'Yes' ELSE 'No' END AS had_support
    FROM activity_summary a
    CROSS JOIN current_reference cr
)
SELECT
    ci.city,
    ci.state_name AS state,
    ci.customer_id,
    ci.full_name,
    ci.email,
    ci.phone,
    ci.signup_date,
    ci.last_activity_date,
    ci.days_inactive,
    ci.had_transactions,
    ci.had_sessions,
    ci.had_support,
    COALESCE(clv.customer_lifetime_value, 0) AS lifetime_value
FROM churn_identification ci
LEFT JOIN CUSTOMER360.GOLD_LAYER.CUSTOMER_LIFETIME_VALUE clv 
    ON ci.customer_id = clv.customer_id
WHERE ci.days_inactive > 30
ORDER BY ci.state_name, ci.city, ci.days_inactive DESC;

-- ========================================================
-- 7. AVG_TICKET_RESOLUTION_TIME
-- ========================================================
CREATE OR REPLACE VIEW CUSTOMER360.GOLD_LAYER.AVG_TICKET_RESOLUTION_TIME AS
SELECT 
    ds.issue_type,
    AVG(DATEDIFF(HOUR, dd_created.dates, dd_resolved.dates)) AS avg_resolution_time_hours,
    AVG(DATEDIFF(HOUR, dd_created.dates, dd_resolved.dates)) / 24.0 AS avg_resolution_time_days,
    COUNT(*) AS ticket_count
FROM CUSTOMER360.GOLD_LAYER.dim_support ds
JOIN CUSTOMER360.GOLD_LAYER.dim_date dd_created 
    ON ds.created_date_sk = dd_created.date_id_sk
JOIN CUSTOMER360.GOLD_LAYER.dim_date dd_resolved 
    ON ds.resolved_date_sk = dd_resolved.date_id_sk
WHERE ds.resolved_date_sk IS NOT NULL
GROUP BY ds.issue_type
ORDER BY avg_resolution_time_days DESC;

-- ========================================================
-- 8. ISSUE_TYPE_BY_TICKETS
-- ========================================================
CREATE OR REPLACE VIEW CUSTOMER360.GOLD_LAYER.ISSUE_TYPE_BY_TICKETS AS 
SELECT 
    issue_type,
    COUNT(*) AS ticket_count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dim_support) AS percentage
FROM dim_support
GROUP BY issue_type
ORDER BY ticket_count DESC;

-- ========================================================
-- 9. RESOLUTION_TICKET_COUNT
-- ========================================================
CREATE OR REPLACE VIEW CUSTOMER360.GOLD_LAYER.RESOLUTION_TICKET_COUNT AS 
SELECT 
    CASE
        WHEN resolution_time_days <= 0.5 THEN '0-12 hours'
        WHEN resolution_time_days <= 1 THEN '12-24 hours'
        WHEN resolution_time_days <= 2 THEN '1-2 days'
        WHEN resolution_time_days <= 5 THEN '2-5 days'
        ELSE '5+ days'
    END AS resolution_time_bucket,
    COUNT(*) AS ticket_count
FROM dim_support
WHERE satisfaction_score IS NOT NULL
  AND resolution_time_days IS NOT NULL
GROUP BY resolution_time_bucket
ORDER BY MIN(resolution_time_days);

-- ========================================================
-- 10. TICKETS_OVER_TIME
-- ========================================================
CREATE OR REPLACE VIEW CUSTOMER360.GOLD_LAYER.TICKETS_OVER_TIME AS 
SELECT 
    d.years,
    d.months,
    d.month_name,
    AVG(s.resolution_time_days) AS avg_resolution_days,
    COUNT(*) AS ticket_count
FROM dim_support s
JOIN dim_date d 
    ON s.created_date_sk = d.date_id_sk
WHERE s.resolved_date_sk IS NOT NULL
GROUP BY d.years, d.months, d.month_name
ORDER BY d.years, d.months;

-- ========================================================
-- 11. PERFORMANCE_BY_SESSION
-- ========================================================
CREATE OR REPLACE VIEW CUSTOMER360.GOLD_LAYER.PERFORMANCE_BY_SESSION AS
SELECT 
    session_category,
    COUNT(*) AS total_sessions,
    AVG(session_duration) AS avg_duration,
    AVG(page_views) AS avg_pages,
    COUNT(DISTINCT customer_id_sk) AS unique_users
FROM dim_sessions
GROUP BY session_category
ORDER BY total_sessions DESC;

-- ========================================================
-- 12. WEB_SESSION_COUNT_BY_DEVICE
-- ========================================================
CREATE OR REPLACE VIEW CUSTOMER360.GOLD_LAYER.WEB_SESSION_COUNT_BY_DEVICE AS
SELECT 
    ds.device_type,
    COUNT(ds.session_id) AS session_count
FROM CUSTOMER360.GOLD_LAYER.dim_sessions ds
GROUP BY ds.device_type
ORDER BY session_count DESC;

-- ========================================================
-- 13. VIEW_CUSTOMER_PURCHASE_FREQUENCY_MONTHLY
-- ========================================================
CREATE OR REPLACE VIEW customer360.gold_layer.VIEW_CUSTOMER_PURCHASE_FREQUENCY_MONTHLY AS
SELECT
    dc.customer_id,
    dd.years,
    dd.months,
    COUNT(DISTINCT ft.transaction_id) AS purchase_count
FROM customer360.gold_layer.fact_transactions ft
JOIN customer360.gold_layer.dim_customers dc 
    ON ft.customer_id_sk = dc.customer_sk
JOIN customer360.gold_layer.dim_date dd 
    ON ft.transaction_date = dd.date_id_sk
GROUP BY 
    dc.customer_id, 
    dd.years, 
    dd.months
ORDER BY 
    dd.years, 
    dd.months, 
    dc.customer_id;

