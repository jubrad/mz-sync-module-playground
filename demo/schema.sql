-- Slice Command — pizza delivery operations demo
-- Tables: stores, drivers, orders

-- ── Stores ─────────────────────────────────────────────────────────
CREATE TABLE stores (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT NOT NULL,
    state CHAR(2) NOT NULL,
    zipcode TEXT NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE stores REPLICA IDENTITY FULL;

-- ── Drivers ────────────────────────────────────────────────────────
CREATE TABLE drivers (
    id SERIAL PRIMARY KEY,
    store_id INT NOT NULL REFERENCES stores(id),
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'available',  -- available | delivering | off_duty
    current_order_id INT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE drivers REPLICA IDENTITY FULL;

-- ── Orders ─────────────────────────────────────────────────────────
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    store_id INT NOT NULL REFERENCES stores(id),
    driver_id INT,
    customer_name TEXT NOT NULL,
    delivery_zipcode TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'placed',
    items TEXT NOT NULL,        -- JSON array of item names
    total_cents INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE orders REPLICA IDENTITY FULL;

-- ── Publication ────────────────────────────────────────────────────
CREATE PUBLICATION demo_pub FOR ALL TABLES;

-- ── Seed stores (30 partner pizza shops) ───────────────────────────
INSERT INTO stores (name, city, state, zipcode) VALUES
    ('Big Slice Pizza',             'New York',      'NY', '10036'),
    ('Cosmic Crust',                'Brooklyn',      'NY', '11201'),
    ('Golden Oven Pizzeria',        'Los Angeles',   'CA', '90028'),
    ('Rustic Pie Co.',              'Santa Monica',  'CA', '90401'),
    ('Hot Corner Pizza',            'Chicago',       'IL', '60601'),
    ('Thin Crust Kitchen',          'Chicago',       'IL', '60622'),
    ('Fire Brick Pies',             'Houston',       'TX', '77002'),
    ('Lucky Tomato',                'Phoenix',       'AZ', '85004'),
    ('Old Hearth Pizzeria',         'Philadelphia',  'PA', '19106'),
    ('Saucy Bros Pizza',            'San Antonio',   'TX', '78205'),
    ('Neon Slice',                  'San Diego',     'CA', '92101'),
    ('Iron Flame Pizza',            'Dallas',        'TX', '75226'),
    ('Honest Pie Shop',             'Austin',        'TX', '78704'),
    ('Crispy Dough Co.',            'Jacksonville',  'FL', '32204'),
    ('Moon Pie Pizzeria',           'San Francisco', 'CA', '94110'),
    ('Red Pepper Pizza',            'Columbus',      'OH', '43215'),
    ('Electric Oven',               'Charlotte',     'NC', '28206'),
    ('True Crust Pizza Bar',        'Indianapolis',  'IN', '46204'),
    ('Stone Table Pies',            'Seattle',       'WA', '98101'),
    ('Mighty Slice Joint',          'Denver',        'CO', '80202'),
    ('Smoky Basil',                 'Nashville',     'TN', '37203'),
    ('Velvet Dough Pizza',          'Portland',      'OR', '97209'),
    ('Atomic Pie',                  'Las Vegas',     'NV', '89109'),
    ('Copper Flame Pizzeria',       'Memphis',       'TN', '38103'),
    ('Lazy Tomato Pizza',           'Louisville',    'KY', '40202'),
    ('Urban Crust',                 'Baltimore',     'MD', '21231'),
    ('Happy Slice Pizza Co.',       'Milwaukee',     'WI', '53202'),
    ('Star Oven Pies',              'Atlanta',       'GA', '30308'),
    ('Cheesy Wheel',                'Miami',         'FL', '33127'),
    ('Royal Brick Pizza',           'Tampa',         'FL', '33605');

-- ── Seed drivers (5 per store = 150 drivers) ──────────────────────
INSERT INTO drivers (store_id, name)
SELECT s.id, d.name
FROM stores s
CROSS JOIN (
    VALUES
        ('Alex Rivera'), ('Jordan Chen'), ('Sam Patel'),
        ('Casey Morgan'), ('Riley Brooks'), ('Taylor Kim'), ('Jamie Lopez')
) AS d(name);

-- ── tick() — advances order lifecycle, assigns drivers, completes
--    deliveries, purges old orders.  Order creation is handled by
--    the Python simulator so it can scale with store count. ────────
CREATE OR REPLACE FUNCTION tick() RETURNS TABLE(status_changes int, deliveries int) AS $$
DECLARE
    v_status_changes int := 0;
    v_deliveries int := 0;
    r RECORD;
BEGIN
    -- ── 1. Advance statuses ───────────────────────────────────────

    -- placed → being_made  (after 2–6s)
    WITH advanced AS (
        UPDATE orders SET status = 'being_made', updated_at = now()
        WHERE status = 'placed'
          AND extract(epoch FROM now() - updated_at) > 2 + random() * 4
        RETURNING 1
    ) SELECT count(*) INTO v_status_changes FROM advanced;

    -- being_made → in_the_oven  (after 4–10s)
    WITH advanced AS (
        UPDATE orders SET status = 'in_the_oven', updated_at = now()
        WHERE status = 'being_made'
          AND extract(epoch FROM now() - updated_at) > 4 + random() * 6
        RETURNING 1
    ) SELECT v_status_changes + count(*) INTO v_status_changes FROM advanced;

    -- in_the_oven → final_prep  (after 6–14s)
    WITH advanced AS (
        UPDATE orders SET status = 'final_prep', updated_at = now()
        WHERE status = 'in_the_oven'
          AND extract(epoch FROM now() - updated_at) > 6 + random() * 8
        RETURNING 1
    ) SELECT v_status_changes + count(*) INTO v_status_changes FROM advanced;

    -- final_prep → waiting_on_driver  (after 2–4s)
    WITH advanced AS (
        UPDATE orders SET status = 'waiting_on_driver', updated_at = now()
        WHERE status = 'final_prep'
          AND extract(epoch FROM now() - updated_at) > 2 + random() * 2
        RETURNING 1
    ) SELECT v_status_changes + count(*) INTO v_status_changes FROM advanced;

    -- ── 2. Assign drivers to waiting orders ───────────────────────
    FOR r IN
        SELECT o.id AS order_id, (
            SELECT d.id FROM drivers d
            WHERE d.store_id = o.store_id AND d.status = 'available'
            ORDER BY d.updated_at LIMIT 1
        ) AS driver_id
        FROM orders o
        WHERE o.status = 'waiting_on_driver' AND o.driver_id IS NULL
        ORDER BY o.updated_at
    LOOP
        IF r.driver_id IS NOT NULL THEN
            UPDATE orders SET driver_id = r.driver_id, status = 'on_its_way', updated_at = now()
            WHERE id = r.order_id;
            UPDATE drivers SET status = 'delivering', current_order_id = r.order_id, updated_at = now()
            WHERE id = r.driver_id;
            v_status_changes := v_status_changes + 1;
        END IF;
    END LOOP;

    -- ── 3. Complete deliveries  (on_its_way for 20–40s) ───────────
    FOR r IN
        SELECT id, driver_id FROM orders
        WHERE status = 'on_its_way'
          AND extract(epoch FROM now() - updated_at) > 20 + random() * 20
    LOOP
        UPDATE orders SET status = 'delivered', updated_at = now()
        WHERE id = r.id;
        IF r.driver_id IS NOT NULL THEN
            UPDATE drivers SET status = 'available', current_order_id = NULL, updated_at = now()
            WHERE id = r.driver_id;
        END IF;
        v_deliveries := v_deliveries + 1;
    END LOOP;

    -- ── 4. Purge old delivered orders  (> 30s) ────────────────────
    DELETE FROM orders
    WHERE status = 'delivered'
      AND updated_at < now() - interval '30 seconds';

    RETURN QUERY SELECT v_status_changes, v_deliveries;
END;
$$ LANGUAGE plpgsql;
