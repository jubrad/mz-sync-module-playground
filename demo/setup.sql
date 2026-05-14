-- Materialize objects for the Fresh Pizza demo.
-- Run against port 6878 (mapped to MZ 6875 inside docker).

-- 1. Secret + connection + source
CREATE SECRET pgpass AS 'postgres';

CREATE CONNECTION pg_conn TO POSTGRES (
    HOST 'postgres',
    PORT 5432,
    USER 'postgres',
    PASSWORD SECRET pgpass,
    DATABASE 'demo'
);

CREATE SOURCE demo_source
    FROM POSTGRES CONNECTION pg_conn
    (PUBLICATION 'demo_pub')
    FOR TABLES (stores, drivers, orders);

-- 2. Materialized views

-- All active orders with store + driver info
CREATE MATERIALIZED VIEW live_orders AS
SELECT
    o.id,
    o.customer_name,
    o.delivery_zipcode,
    o.status,
    o.items,
    o.total_cents,
    o.store_id,
    s.name   AS store_name,
    s.city   AS store_city,
    s.state  AS store_state,
    COALESCE(d.name, '')  AS driver_name,
    o.created_at,
    o.updated_at
FROM orders o
JOIN stores s ON o.store_id = s.id
LEFT JOIN drivers d ON o.driver_id = d.id
WHERE o.status != 'delivered';

CREATE DEFAULT INDEX ON live_orders;

-- Per-store operational dashboard
CREATE MATERIALIZED VIEW store_dashboard AS
SELECT
    s.id,
    s.name,
    s.city,
    s.state,
    s.zipcode,
    count(o.id) FILTER (WHERE o.status != 'delivered')          AS active_orders,
    count(o.id) FILTER (WHERE o.status = 'placed')              AS placed,
    count(o.id) FILTER (WHERE o.status = 'being_made')          AS making,
    count(o.id) FILTER (WHERE o.status = 'in_the_oven')         AS baking,
    count(o.id) FILTER (WHERE o.status = 'final_prep')          AS prepping,
    count(o.id) FILTER (WHERE o.status = 'waiting_on_driver')   AS waiting,
    count(o.id) FILTER (WHERE o.status = 'on_its_way')          AS delivering,
    count(o.id)                                                 AS total_orders
FROM stores s
LEFT JOIN orders o ON o.store_id = s.id
GROUP BY s.id, s.name, s.city, s.state, s.zipcode;

CREATE DEFAULT INDEX ON store_dashboard;

-- Driver board
CREATE MATERIALIZED VIEW driver_board AS
SELECT
    d.id,
    d.name,
    d.status,
    s.name        AS store_name,
    s.city        AS store_city,
    s.state       AS store_state,
    COALESCE(o.customer_name, '')   AS current_customer,
    COALESCE(o.delivery_zipcode, '') AS current_delivery_zip,
    d.updated_at
FROM drivers d
JOIN stores s ON d.store_id = s.id
LEFT JOIN orders o ON d.current_order_id = o.id;

CREATE DEFAULT INDEX ON driver_board;

-- Recently delivered (stays bounded — old rows purged by tick())
CREATE MATERIALIZED VIEW recent_deliveries AS
SELECT
    o.id,
    o.store_id,
    o.customer_name,
    o.items,
    o.total_cents,
    s.name        AS store_name,
    s.city        AS store_city,
    s.state       AS store_state,
    COALESCE(d.name, '')  AS driver_name,
    o.created_at,
    o.updated_at  AS delivered_at
FROM orders o
JOIN stores s ON o.store_id = s.id
LEFT JOIN drivers d ON o.driver_id = d.id
WHERE o.status = 'delivered';

CREATE DEFAULT INDEX ON recent_deliveries;

-- Pre-computed: top selling item combos (for overview panel)
CREATE MATERIALIZED VIEW top_sellers AS
SELECT
    o.items,
    count(*) AS cnt
FROM orders o
WHERE o.status != 'delivered'
GROUP BY o.items;

CREATE DEFAULT INDEX ON top_sellers;

-- Pre-computed: slow orders per store (orders active > 45 seconds)
-- Uses mz_now() temporal filter — MZ maintains this incrementally
CREATE MATERIALIZED VIEW slow_orders_by_store AS
SELECT
    o.store_id,
    s.name     AS store_name,
    s.city     AS store_city,
    s.state    AS store_state,
    count(*)   AS slow_count
FROM orders o
JOIN stores s ON o.store_id = s.id
WHERE o.status != 'delivered'
  AND mz_now() >= o.created_at + INTERVAL '45 seconds'
GROUP BY o.store_id, s.name, s.city, s.state;

CREATE DEFAULT INDEX ON slow_orders_by_store;

-- Pre-computed: slow individual orders (for store detail panel)
CREATE MATERIALIZED VIEW slow_orders AS
SELECT
    o.id,
    o.store_id,
    o.customer_name,
    o.status
FROM orders o
WHERE o.status != 'delivered'
  AND mz_now() >= o.created_at + INTERVAL '45 seconds';

CREATE DEFAULT INDEX ON slow_orders;
