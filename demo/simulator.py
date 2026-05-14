"""
Slice Command Simulator — FastAPI server that drives the demo.

Creates orders proportional to store count (~10 concurrent per store),
advances the order lifecycle via tick(), and exposes an API to open
new stores on the fly.

    uvicorn simulator:app --port 9000
"""

import asyncio
import random
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ── Config ──────────────────────────────────────────────────────────

DB_DSN = "postgresql://postgres:postgres@localhost:5488/demo"
TICK_INTERVAL = 0.25  # seconds (4 ticks/sec)
MIN_ORDERS_PER_MIN = 10  # per store
MAX_ORDERS_PER_MIN = 30  # per store

# ── Data for random order generation ────────────────────────────────

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Chris",
    "Lisa", "Daniel", "Nancy", "Matt", "Betty", "Tony", "Margaret", "Mark",
    "Sandra", "Steve", "Ashley", "Paul", "Donna", "Andrew", "Emily", "Josh",
    "Kimberly", "Nick", "Megan", "Brian", "Rachel", "Kevin", "Samantha",
    "Jason", "Lauren", "Ryan", "Heather", "Tyler", "Amber", "Brandon",
    "Nicole", "Jake", "Tiffany", "Aaron", "Brittany", "Ben", "Hannah",
    "Zach", "Alexis", "Dylan", "Olivia", "Ethan", "Sophia", "Connor",
    "Emma", "Mason", "Ava", "Logan", "Isabella",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
    "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz",
]

MENU_ITEMS = [
    '["Large Pepperoni", "Garlic Bread"]',
    '["Medium Margherita", "Caesar Salad", "2L Coke"]',
    '["XL Meat Lovers", "Buffalo Wings", "Cheese Sticks"]',
    '["Large Hawaiian", "Breadsticks"]',
    '["Medium Veggie Supreme", "Side Salad"]',
    '["Large BBQ Chicken", "Onion Rings", "2L Sprite"]',
    '["XL Supreme", "Large Pepperoni", "Garlic Knots", "2L Dr Pepper"]',
    '["Medium Four Cheese", "Tiramisu"]',
    '["Large Chicken Alfredo", "Mozzarella Sticks"]',
    '["XL The Works", "Buffalo Wings", "Cookie Brownie", "2L Mountain Dew"]',
    '["Medium White Pizza", "Greek Salad"]',
    '["Large Sausage & Mushroom", "Cheesy Bread", "Cinnamon Twists"]',
]

DRIVER_NAMES = [
    "Alex Rivera", "Jordan Chen", "Sam Patel", "Casey Morgan",
    "Riley Brooks", "Taylor Kim", "Jamie Lopez",
]

# Pizza shop name generator — combinatorial, produces thousands of unique names
PREFIXES = [
    "Big", "Little", "Old", "New", "True", "Hot", "Fat", "Thin", "Red",
    "Golden", "Lucky", "Cosmic", "Midnight", "Rustic", "Urban", "Royal",
    "Classic", "Honest", "Crispy", "Saucy", "Cheesy", "Smoky", "Crazy",
    "Happy", "Lazy", "Mighty", "Atomic", "Electric", "Velvet", "Iron",
    "Neon", "Copper", "Stone", "Fire", "Moon", "Star", "Sun", "River",
]
NOUNS = [
    "Slice", "Pie", "Crust", "Oven", "Table", "Tomato", "Pepper",
    "Basil", "Dough", "Flame", "Brick", "Corner", "Bros", "Joint",
    "Kitchen", "House", "Room", "Spot", "Stop", "Wheel", "Bell",
    "Fork", "Plate", "Board", "Box", "Peel", "Hearth",
]
SUFFIXES = [
    "Pizza", "Pizzeria", "Pizza Co.", "Pies", "Pizza Bar", "Pizza Shop",
    "Pizza Kitchen", "Slice House", "Pie Shop", "Pizza Joint",
    "", "", "", "",  # often no suffix — just "Big Slice" or "Cosmic Dough"
]

CITIES = [
    ("New York", "NY", "10036"), ("Brooklyn", "NY", "11201"),
    ("Los Angeles", "CA", "90028"), ("Santa Monica", "CA", "90401"),
    ("Chicago", "IL", "60601"), ("Houston", "TX", "77002"),
    ("Phoenix", "AZ", "85004"), ("Philadelphia", "PA", "19106"),
    ("San Antonio", "TX", "78205"), ("San Diego", "CA", "92101"),
    ("Dallas", "TX", "75226"), ("Austin", "TX", "78704"),
    ("Jacksonville", "FL", "32204"), ("San Francisco", "CA", "94110"),
    ("Columbus", "OH", "43215"), ("Charlotte", "NC", "28206"),
    ("Indianapolis", "IN", "46204"), ("Seattle", "WA", "98101"),
    ("Denver", "CO", "80202"), ("Nashville", "TN", "37203"),
    ("Portland", "OR", "97209"), ("Las Vegas", "NV", "89109"),
    ("Memphis", "TN", "38103"), ("Louisville", "KY", "40202"),
    ("Baltimore", "MD", "21231"), ("Milwaukee", "WI", "53202"),
    ("Atlanta", "GA", "30308"), ("Miami", "FL", "33127"),
    ("Tampa", "FL", "33605"), ("Washington", "DC", "20036"),
    ("New Orleans", "LA", "70112"), ("Minneapolis", "MN", "55401"),
    ("Kansas City", "MO", "64105"), ("Omaha", "NE", "68102"),
    ("Raleigh", "NC", "27601"), ("Birmingham", "AL", "35203"),
    ("St. Louis", "MO", "63130"), ("San Jose", "CA", "95113"),
    ("Pittsburgh", "PA", "15222"), ("Richmond", "VA", "23221"),
    ("Providence", "RI", "02903"), ("Salt Lake City", "UT", "84101"),
    ("Cincinnati", "OH", "45202"), ("Tucson", "AZ", "85719"),
    ("Boise", "ID", "83706"), ("Boston", "MA", "02116"),
    ("Savannah", "GA", "31401"), ("Charleston", "SC", "29401"),
    ("Asheville", "NC", "28801"), ("Fort Worth", "TX", "76102"),
]

_used_names: set[str] = set()


def generate_store() -> tuple[str, str, str, str]:
    """Generate a unique pizza shop name + random city. ~40k possible combos."""
    for _ in range(2000):
        prefix = random.choice(PREFIXES)
        noun = random.choice(NOUNS)
        suffix = random.choice(SUFFIXES)
        name = f"{prefix} {noun} {suffix}".strip() if suffix else f"{prefix} {noun}"
        if name not in _used_names:
            _used_names.add(name)
            city, state, zipcode = random.choice(CITIES)
            return (name, city, state, zipcode)
    # Fallback
    n = len(_used_names) + 1
    city, state, zipcode = random.choice(CITIES)
    return (f"Pizza Place #{n}", city, state, zipcode)


# ── App state ───────────────────────────────────────────────────────

pool: asyncpg.Pool
sim_task: asyncio.Task
rate_multiplier: float = 1.0  # bumped during sales

# Per-store config: store_id → (orders_per_minute, zipcode)
store_info: dict[int, tuple[float, str]] = {}

# Rolling window for orders-per-second display (last 10 ticks)
_tick_counts: list[int] = []


def _random_rate() -> float:
    """Random order rate for a new store: 10–30 orders/minute."""
    return random.uniform(MIN_ORDERS_PER_MIN, MAX_ORDERS_PER_MIN)


# ── Helpers ─────────────────────────────────────────────────────────

async def load_stores():
    """Load all existing stores into store_info with random rates."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, zipcode FROM stores")
    for row in rows:
        store_info[row["id"]] = (_random_rate(), row["zipcode"])
    print(f"[simulator] loaded {len(rows)} stores with rates {MIN_ORDERS_PER_MIN}–{MAX_ORDERS_PER_MIN} orders/min")


def register_store(store_id: int, zipcode: str) -> float:
    """Register a new store and return its assigned rate."""
    rate = _random_rate()
    store_info[store_id] = (rate, zipcode)
    return rate


def generate_order_rows(count: int, store_id: int, zipcode: str) -> list[tuple]:
    """Generate order row tuples for batch insert."""
    return [
        (
            store_id,
            f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            zipcode,
            random.choice(MENU_ITEMS),
            random.randint(1199, 4999),
        )
        for _ in range(count)
    ]


async def create_orders(conn: asyncpg.Connection, count: int, store_id: int, zipcode: str):
    """Insert `count` random orders for a specific store."""
    if count <= 0:
        return
    rows = generate_order_rows(count, store_id, zipcode)
    await conn.executemany(
        """INSERT INTO orders (store_id, customer_name, delivery_zipcode, status, items, total_cents)
           VALUES ($1, $2, $3, 'placed', $4, $5)""",
        rows,
    )


def orders_per_second() -> float:
    """Rolling average orders/sec over the last N ticks."""
    if not _tick_counts:
        return 0.0
    window_secs = len(_tick_counts) * TICK_INTERVAL
    return sum(_tick_counts) / window_secs if window_secs > 0 else 0.0


async def simulation_loop():
    """Main simulation tick — runs every TICK_INTERVAL seconds."""
    while True:
        try:
            # Collect all order rows across all stores in-memory first
            all_rows: list[tuple] = []
            for sid, (rate_per_min, zipcode) in store_info.items():
                rate_per_tick = rate_per_min / 60.0 * TICK_INTERVAL * rate_multiplier
                n = int(rate_per_tick)
                if random.random() < (rate_per_tick - n):
                    n += 1
                if n > 0:
                    all_rows.extend(generate_order_rows(n, sid, zipcode))

            async with pool.acquire() as conn:
                # Single batch insert for all new orders
                if all_rows:
                    await conn.executemany(
                        """INSERT INTO orders (store_id, customer_name, delivery_zipcode, status, items, total_cents)
                           VALUES ($1, $2, $3, 'placed', $4, $5)""",
                        all_rows,
                    )
                await conn.execute("SELECT * FROM tick()")

            _tick_counts.append(len(all_rows))
            if len(_tick_counts) > 20:
                _tick_counts.pop(0)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[simulator] tick error: {e}")
        await asyncio.sleep(TICK_INTERVAL)


# ── FastAPI app ─────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(_app: FastAPI):
    global pool, sim_task
    pool = await asyncpg.create_pool(DB_DSN, min_size=2, max_size=10)
    await load_stores()
    sim_task = asyncio.create_task(simulation_loop())
    print("[simulator] running — creating orders + advancing lifecycle every 1s")
    yield
    sim_task.cancel()
    try:
        await sim_task
    except asyncio.CancelledError:
        pass
    await pool.close()

app = FastAPI(title="Slice Command Simulator", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/sim/status")
async def status():
    """Simulation overview: store count, orders by status, driver stats."""
    async with pool.acquire() as conn:
        stores = await conn.fetchval("SELECT count(*) FROM stores")
        order_rows = await conn.fetch(
            "SELECT status, count(*) AS cnt FROM orders GROUP BY status"
        )
        driver_rows = await conn.fetch(
            "SELECT status, count(*) AS cnt FROM drivers GROUP BY status"
        )
        total_revenue = await conn.fetchval(
            "SELECT coalesce(sum(total_cents), 0) FROM orders"
        )
    return {
        "stores": stores,
        "orders": {r["status"]: r["cnt"] for r in order_rows},
        "drivers": {r["status"]: r["cnt"] for r in driver_rows},
        "total_revenue_cents": total_revenue,
        "rate_multiplier": rate_multiplier,
        "sale_active": rate_multiplier > 1.0,
        "orders_per_second": round(orders_per_second(), 1),
    }


@app.post("/sim/stores")
async def open_stores(count: int = 1):
    """Open stores in a single SQL transaction."""
    count = max(1, min(count, 500))

    # Generate all data in-memory first (no DB calls)
    new_stores = [generate_store() for _ in range(count)]

    async with pool.acquire() as conn:
        # Insert all stores in one executemany, get IDs back
        store_ids = await conn.fetch(
            "INSERT INTO stores (name, city, state, zipcode) " +
            "SELECT u.name, u.city, u.state, u.zipcode FROM unnest($1::text[], $2::text[], $3::text[], $4::text[]) AS u(name, city, state, zipcode) " +
            "RETURNING id, zipcode",
            [s[0] for s in new_stores],
            [s[1] for s in new_stores],
            [s[2] for s in new_stores],
            [s[3] for s in new_stores],
        )

        # Bulk insert all drivers (5 per store)
        driver_rows = []
        for row in store_ids:
            for dname in DRIVER_NAMES:
                driver_rows.append((row["id"], dname))
        await conn.executemany(
            "INSERT INTO drivers (store_id, name) VALUES ($1, $2)",
            driver_rows,
        )

        # Bulk insert all seed orders (10 per store)
        all_order_rows: list[tuple] = []
        for row in store_ids:
            all_order_rows.extend(generate_order_rows(10, row["id"], row["zipcode"]))
        if all_order_rows:
            await conn.executemany(
                """INSERT INTO orders (store_id, customer_name, delivery_zipcode, status, items, total_cents)
                   VALUES ($1, $2, $3, 'placed', $4, $5)""",
                all_order_rows,
            )

    # Register all stores in simulator state
    opened = []
    for (name, city, state, zipcode), row in zip(new_stores, store_ids):
        rate = register_store(row["id"], row["zipcode"])
        opened.append({"store_id": row["id"], "name": name, "city": city, "state": state, "orders_per_minute": round(rate, 1)})

    total = await pool.fetchval("SELECT count(*) FROM stores")
    print(f"[simulator] opened {count} store(s) — {total} total")

    return {
        "stores_opened": len(opened),
        "stores": opened,
        "total_stores": total,
    }


@app.post("/sim/burst/{store_id}")
async def burst_orders(store_id: int, count: int = 10):
    """Send a burst of orders to a specific store."""
    info = store_info.get(store_id)
    if not info:
        return {"error": "Store not found"}, 404
    async with pool.acquire() as conn:
        await create_orders(conn, count, store_id, info[1])
    return {"store_id": store_id, "orders_created": count}


@app.post("/sim/sale")
async def toggle_sale(multiplier: float = 2.0):
    """Toggle a sale. Pass multiplier=10 for super sale."""
    global rate_multiplier
    if rate_multiplier > 1.0:
        rate_multiplier = 1.0
        print("[simulator] sale ended — back to normal rate")
    else:
        rate_multiplier = max(1.0, min(multiplier, 50.0))
        print(f"[simulator] SALE STARTED — {rate_multiplier}x order rate!")
    return {"sale_active": rate_multiplier > 1.0, "rate_multiplier": rate_multiplier}


@app.post("/sim/reset")
async def reset():
    """Wipe all orders, remove expansion stores/drivers, reset to initial 30."""
    global _used_names
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM orders")
        await conn.execute("DELETE FROM drivers WHERE store_id > 30")
        await conn.execute("DELETE FROM stores WHERE id > 30")
        await conn.execute("UPDATE drivers SET status='available', current_order_id=NULL, updated_at=now()")

    store_info.clear()
    _used_names = set()
    _tick_counts.clear()
    await load_stores()
    print("[simulator] RESET — back to 30 stores")
    return {"stores": len(store_info), "status": "reset"}


@app.post("/sim/drivers/{store_id}")
async def adjust_drivers(store_id: int, count: int = 1):
    """Hire (positive count) or fire (negative count) drivers at a store."""
    async with pool.acquire() as conn:
        exists = await conn.fetchval("SELECT 1 FROM stores WHERE id = $1", store_id)
        if not exists:
            return {"error": "Store not found"}, 404

        if count > 0:
            # Hire drivers
            hired = []
            for i in range(count):
                name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
                did = await conn.fetchval(
                    "INSERT INTO drivers (store_id, name) VALUES ($1, $2) RETURNING id",
                    store_id, name,
                )
                hired.append({"id": did, "name": name})
            total = await conn.fetchval(
                "SELECT count(*) FROM drivers WHERE store_id = $1", store_id
            )
            print(f"[simulator] hired {count} driver(s) for store {store_id} — {total} total")
            return {"store_id": store_id, "hired": len(hired), "total_drivers": total}
        else:
            # Fire drivers (only idle ones)
            to_fire = min(abs(count), await conn.fetchval(
                "SELECT count(*) FROM drivers WHERE store_id = $1 AND status = 'available'", store_id
            ))
            if to_fire > 0:
                await conn.execute(
                    """DELETE FROM drivers WHERE id IN (
                        SELECT id FROM drivers WHERE store_id = $1 AND status = 'available'
                        ORDER BY updated_at LIMIT $2
                    )""",
                    store_id, to_fire,
                )
            total = await conn.fetchval(
                "SELECT count(*) FROM drivers WHERE store_id = $1", store_id
            )
            print(f"[simulator] fired {to_fire} driver(s) from store {store_id} — {total} total")
            return {"store_id": store_id, "fired": to_fire, "total_drivers": total}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000)
