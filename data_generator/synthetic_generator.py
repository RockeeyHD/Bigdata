"""
synthetic_generator.py
Data Synthesizer - E-Commerce & Retail Analytics (Milestone 1)

Menghasilkan 3 dataset terintegrasi:
  1. transactions.csv  (Fact, >= 1.000.000 baris)
  2. customers.csv     (Dimension, 50.000 - 100.000 baris)
  3. clickstream.json  (Semi-structured, JSON Lines, 200.000 - 500.000 event)

Injeksi data kotor (Veracity Check)
-----------------------------------
Sebanyak `--dirty-rate` (default 8%, rentang yang diminta 5% - 10%) dari baris
sengaja dirusak. Setiap baris kotor hanya mendapat SATU jenis anomali (alokasi
disjoint), sehingga persentase total baris kotor dapat dihitung dan diverifikasi:
  - Missing values          : customer_id, timestamp, category (null)
  - Inconsistent formatting : nama kota (Jakarta / JKT / jakarta / DKI Jakarta ...)
  - Outliers & invalid      : price < 0, quantity <= 0, tanggal di masa depan
  - Duplicate records       : transaction_id / event_id / customer_id ganda

Pemakaian:
  python synthetic_generator.py --out-dir ../data/raw
  python synthetic_generator.py --n-transactions 2000000 --n-customers 80000 \
        --n-events 400000 --dirty-rate 0.07 --seed 42
"""
import argparse
import json
import os
import time

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------- #
# Konfigurasi domain
# --------------------------------------------------------------------------- #
CITIES = {  # nama kanonik -> [kanonik, variasi kotor ...]
    "Jakarta": ["Jakarta", "jakarta", "JKT", "DKI Jakarta", "JAKARTA", "Jkt "],
    "Bandung": ["Bandung", "bandung", "BDG", "Kota Bandung", "BANDUNG"],
    "Surabaya": ["Surabaya", "surabaya", "SBY", "Kota Surabaya", "SURABAYA"],
    "Medan": ["Medan", "medan", "MDN", "Kota Medan", "MEDAN"],
    "Semarang": ["Semarang", "semarang", "SMG", "Kota Semarang", "SEMARANG"],
    "Yogyakarta": ["Yogyakarta", "yogyakarta", "Jogja", "DIY", "YOGYAKARTA"],
    "Makassar": ["Makassar", "makassar", "MKS", "Ujung Pandang", "MAKASSAR"],
    "Denpasar": ["Denpasar", "denpasar", "DPS", "Bali", "DENPASAR"],
    "Tangerang": ["Tangerang", "tangerang", "TNG", "Kota Tangerang", "TANGERANG"],
    "Bekasi": ["Bekasi", "bekasi", "BKS", "Kota Bekasi", "BEKASI"],
}
CITY_NAMES = list(CITIES.keys())
CITY_WEIGHTS = np.array([0.28, 0.12, 0.11, 0.07, 0.06, 0.06, 0.05, 0.04, 0.11, 0.10])

CATEGORIES = {  # kategori -> (harga min, harga max) dalam Rupiah
    "Electronics": (150_000, 15_000_000),
    "Fashion": (40_000, 1_500_000),
    "Home & Living": (30_000, 3_000_000),
    "Beauty": (20_000, 800_000),
    "Sports": (50_000, 2_500_000),
    "Books": (25_000, 300_000),
    "Groceries": (5_000, 250_000),
    "Toys": (30_000, 900_000),
}
CAT_NAMES = list(CATEGORIES.keys())
CAT_WEIGHTS = np.array([0.15, 0.22, 0.12, 0.12, 0.08, 0.06, 0.17, 0.08])

PAYMENT = ["Credit Card", "Debit Card", "E-Wallet", "Bank Transfer", "COD", "PayLater"]
PAYMENT_W = np.array([0.15, 0.12, 0.35, 0.18, 0.12, 0.08])

AGE_GROUPS = ["18-24", "25-34", "35-44", "45-54", "55+"]
AGE_W = np.array([0.22, 0.38, 0.22, 0.12, 0.06])
GENDERS = ["Male", "Female"]
TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
TIER_W = np.array([0.55, 0.28, 0.13, 0.04])

PAGES = ["/home", "/search", "/category", "/product", "/cart", "/checkout", "/payment", "/account"]
PAGE_W = np.array([0.18, 0.17, 0.15, 0.25, 0.10, 0.06, 0.04, 0.05])
ACTIONS_BY_PAGE = {
    "/home": ["page_view", "click_banner"],
    "/search": ["search", "page_view"],
    "/category": ["page_view", "filter"],
    "/product": ["view_product", "add_to_wishlist", "page_view"],
    "/cart": ["add_to_cart", "remove_from_cart", "page_view"],
    "/checkout": ["begin_checkout"],
    "/payment": ["purchase", "payment_failed"],
    "/account": ["login", "page_view"],
}
DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_W = np.array([0.62, 0.32, 0.06])
DEVICE_VARIANTS = {
    "mobile": ["mobile", "Mobile", "MOBILE", "android", "ios"],
    "desktop": ["desktop", "Desktop", "DESKTOP", "pc", "web"],
    "tablet": ["tablet", "Tablet", "TABLET", "ipad"],
}

START = pd.Timestamp("2024-01-01")
END = pd.Timestamp("2025-12-31 23:59:59")

# Proporsi jenis anomali di dalam baris kotor (tiap dict berjumlah 1.0)
W_TRX = {"city_format": 0.36, "null_customer_id": 0.09, "null_timestamp": 0.07,
         "null_category": 0.09, "negative_price": 0.07, "invalid_quantity": 0.05,
         "future_timestamp": 0.07, "duplicate": 0.20}
W_CUST = {"city_format": 0.35, "gender_format": 0.20, "null_age_group": 0.12,
          "null_city": 0.08, "null_signup_date": 0.08, "future_signup_date": 0.07,
          "duplicate": 0.10}
W_CLICK = {"device_format": 0.35, "null_user_id": 0.20, "null_timestamp": 0.12,
           "null_action_event": 0.08, "future_timestamp": 0.10, "duplicate": 0.15}


# --------------------------------------------------------------------------- #
# Helper
# --------------------------------------------------------------------------- #
def random_timestamps(rng, n, start=START, end=END):
    """Timestamp acak dengan pola jam sibuk (puncak sore - malam)."""
    span = int((end - start).total_seconds())
    secs = rng.integers(0, span, size=n)
    ts = start + pd.to_timedelta(secs, unit="s")
    hour_w = np.array([1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 6, 7,
                       7, 6, 6, 6, 7, 8, 9, 10, 10, 8, 5, 3], dtype=float)
    hour = rng.choice(np.arange(24), size=n, p=hour_w / hour_w.sum())
    ts = ts.normalize() + pd.to_timedelta(hour, unit="h") \
        + pd.to_timedelta(rng.integers(0, 3600, size=n), unit="s")
    return pd.Series(ts)


def allocate(rng, n, weights, dirty_rate):
    """Bagi dirty_rate * n baris ke jenis anomali secara disjoint (tanpa tumpang tindih)."""
    total = int(round(n * dirty_rate))
    perm = rng.permutation(n)
    out, pos = {}, 0
    keys = list(weights)
    for i, k in enumerate(keys):
        cnt = int(round(total * weights[k])) if i < len(keys) - 1 else total - pos
        out[k] = perm[pos:pos + cnt]
        pos += cnt
    return out


def apply_variants(rng, values, variants_map):
    """Ganti nilai kanonik dengan variasi penulisan acak (case, singkatan, awalan)."""
    values = np.asarray(values, dtype=object)
    out = values.copy()
    for canon, variants in variants_map.items():
        m = values == canon
        if m.any():
            out[m] = rng.choice(variants[1:], size=int(m.sum()))
    return out


def future_dates(rng, k, fmt, start="2027-01-01", span_days=4 * 365):
    d = pd.Timestamp(start) + pd.to_timedelta(rng.integers(0, span_days, size=k), unit="D") \
        + pd.to_timedelta(rng.integers(0, 86400, size=k), unit="s")
    return np.array(d.strftime(fmt), dtype=object)


def finish(df, blk, log, seed):
    """Tambahkan baris duplikat, acak urutan, hitung statistik kekotoran."""
    dup = df.iloc[blk["duplicate"]].copy()
    n_base = len(df)
    df = pd.concat([df, dup], ignore_index=True).sample(frac=1, random_state=seed).reset_index(drop=True)
    dirty = sum(len(v) for k, v in blk.items() if k != "duplicate") + len(dup)
    log.update({k: int(len(v)) for k, v in blk.items() if k != "duplicate"})
    log["duplicate_rows"] = int(len(dup))
    log["rows_base"] = int(n_base)
    log["rows_final"] = int(len(df))
    log["dirty_rows"] = int(dirty)
    log["dirty_share_pct"] = round(100 * dirty / len(df), 2)
    return df, log


# --------------------------------------------------------------------------- #
# Generator tiap tabel
# --------------------------------------------------------------------------- #
def gen_customers(rng, n, dirty, seed):
    city_idx = rng.choice(len(CITY_NAMES), size=n, p=CITY_WEIGHTS)
    canon_city = np.array(CITY_NAMES, dtype=object)[city_idx]
    df = pd.DataFrame({
        "customer_id": [f"C{i:07d}" for i in range(1, n + 1)],
        "age_group": rng.choice(AGE_GROUPS, size=n, p=AGE_W),
        "gender": rng.choice(GENDERS, size=n),
        "signup_date": random_timestamps(rng, n, pd.Timestamp("2022-01-01"),
                                         pd.Timestamp("2025-06-30")).dt.strftime("%Y-%m-%d"),
        "membership_tier": rng.choice(TIERS, size=n, p=TIER_W),
        "city": canon_city,
    })
    blk = allocate(rng, n, W_CUST, dirty)
    i = blk["city_format"]
    df.loc[i, "city"] = apply_variants(rng, canon_city[i], CITIES)
    df.loc[blk["gender_format"], "gender"] = rng.choice(
        ["M", "F", "male", "female", "L", "P"], size=len(blk["gender_format"]))
    df.loc[blk["null_age_group"], "age_group"] = None
    df.loc[blk["null_city"], "city"] = None
    df.loc[blk["null_signup_date"], "signup_date"] = None
    df.loc[blk["future_signup_date"], "signup_date"] = future_dates(
        rng, len(blk["future_signup_date"]), "%Y-%m-%d")
    return finish(df, blk, {}, seed)


def gen_transactions(rng, n, n_cust, n_prod, dirty, seed):
    # Master produk: kategori + harga dasar
    prod_cat_idx = rng.choice(len(CAT_NAMES), size=n_prod, p=CAT_WEIGHTS)
    prod_base = np.array([
        np.exp(rng.uniform(np.log(CATEGORIES[CAT_NAMES[c]][0]), np.log(CATEGORIES[CAT_NAMES[c]][1])))
        for c in prod_cat_idx
    ]).round(-2)

    # Pelanggan berdistribusi Pareto -> ada heavy buyer (berguna untuk RFM)
    w = rng.pareto(2.0, size=n_cust) + 1
    w /= w.sum()
    cust_num = rng.choice(n_cust, size=n, p=w) + 1
    prod_num = rng.integers(0, n_prod, size=n)

    qty = rng.choice([1, 2, 3, 4, 5], size=n, p=[0.62, 0.22, 0.09, 0.05, 0.02])
    price = (prod_base[prod_num] * rng.uniform(0.9, 1.1, size=n)).round(-2)
    discount = rng.choice([0, 0.05, 0.10, 0.15, 0.20, 0.30], size=n,
                          p=[0.45, 0.15, 0.15, 0.12, 0.08, 0.05])
    city_idx = rng.choice(len(CITY_NAMES), size=n, p=CITY_WEIGHTS)
    canon_city = np.array(CITY_NAMES, dtype=object)[city_idx]

    df = pd.DataFrame({
        "transaction_id": [f"T{i:09d}" for i in range(1, n + 1)],
        "customer_id": [f"C{c:07d}" for c in cust_num],
        "timestamp": random_timestamps(rng, n).dt.strftime("%Y-%m-%d %H:%M:%S"),
        "product_id": [f"P{p:05d}" for p in prod_num + 1],
        "category": np.array(CAT_NAMES, dtype=object)[prod_cat_idx[prod_num]],
        "quantity": qty,
        "price": price,
        "discount": discount,
        "payment_method": rng.choice(PAYMENT, size=n, p=PAYMENT_W),
        "city": canon_city,
    })
    blk = allocate(rng, n, W_TRX, dirty)
    i = blk["city_format"]
    df.loc[i, "city"] = apply_variants(rng, canon_city[i], CITIES)
    df.loc[blk["null_customer_id"], "customer_id"] = None
    df.loc[blk["null_timestamp"], "timestamp"] = None
    df.loc[blk["null_category"], "category"] = None
    i = blk["negative_price"]
    df.loc[i, "price"] = -df.loc[i, "price"].abs()
    df.loc[blk["invalid_quantity"], "quantity"] = rng.choice(
        [-3, -2, -1, 0], size=len(blk["invalid_quantity"]))
    df.loc[blk["future_timestamp"], "timestamp"] = future_dates(
        rng, len(blk["future_timestamp"]), "%Y-%m-%d %H:%M:%S")
    return finish(df, blk, {}, seed)


def gen_clickstream(rng, n, n_cust, dirty, seed):
    n_sessions = max(n // 6, 1000)
    sess_num = rng.integers(1, n_sessions + 1, size=n)
    sess_user = rng.integers(1, n_cust + 1, size=n_sessions + 1)  # sesi -> user (stabil)
    page = rng.choice(PAGES, size=n, p=PAGE_W)
    action = np.empty(n, dtype=object)
    for p, acts in ACTIONS_BY_PAGE.items():
        m = page == p
        action[m] = rng.choice(acts, size=int(m.sum()))
    device = rng.choice(DEVICES, size=n, p=DEVICE_W).astype(object)

    df = pd.DataFrame({
        "event_id": [f"E{i:08d}" for i in range(1, n + 1)],
        "session_id": [f"S{s:07d}" for s in sess_num],
        "user_id": [f"C{sess_user[s]:07d}" for s in sess_num],
        "timestamp": random_timestamps(rng, n).dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "page_url": page,
        "action_event": action,
        "device_type": device,
    })
    blk = allocate(rng, n, W_CLICK, dirty)
    i = blk["device_format"]
    df.loc[i, "device_type"] = apply_variants(rng, device[i], DEVICE_VARIANTS)
    df.loc[blk["null_user_id"], "user_id"] = None
    df.loc[blk["null_timestamp"], "timestamp"] = None
    df.loc[blk["null_action_event"], "action_event"] = None
    df.loc[blk["future_timestamp"], "timestamp"] = future_dates(
        rng, len(blk["future_timestamp"]), "%Y-%m-%dT%H:%M:%S", start="2027-01-01")
    return finish(df, blk, {}, seed)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(description="Synthetic E-Commerce data generator")
    ap.add_argument("--out-dir", default="../data/raw")
    ap.add_argument("--n-transactions", type=int, default=1_500_000)
    ap.add_argument("--n-customers", type=int, default=75_000)
    ap.add_argument("--n-events", type=int, default=300_000)
    ap.add_argument("--n-products", type=int, default=5_000)
    ap.add_argument("--dirty-rate", type=float, default=0.08,
                    help="Porsi baris kotor (0.05 - 0.10 sesuai ketentuan)")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()

    if not 0.05 <= a.dirty_rate <= 0.10:
        print(f"[PERINGATAN] dirty-rate {a.dirty_rate} di luar rentang 5%-10% yang diminta.")
    os.makedirs(a.out_dir, exist_ok=True)
    rng = np.random.default_rng(a.seed)
    t0 = time.time()

    print("[1/3] Generating customers ...")
    cust, cust_log = gen_customers(rng, a.n_customers, a.dirty_rate, a.seed)
    cust.to_csv(os.path.join(a.out_dir, "customers.csv"), index=False)

    print("[2/3] Generating transactions ...")
    trx, trx_log = gen_transactions(rng, a.n_transactions, a.n_customers, a.n_products,
                                    a.dirty_rate, a.seed)
    trx.to_csv(os.path.join(a.out_dir, "transactions.csv"), index=False)

    print("[3/3] Generating clickstream (JSON Lines) ...")
    click, click_log = gen_clickstream(rng, a.n_events, a.n_customers, a.dirty_rate, a.seed)
    click.to_json(os.path.join(a.out_dir, "clickstream.json"), orient="records", lines=True)

    summary = {"seed": a.seed, "dirty_rate_param": a.dirty_rate,
               "customers": cust_log, "transactions": trx_log, "clickstream": click_log,
               "elapsed_sec": round(time.time() - t0, 1)}
    with open(os.path.join(a.out_dir, "injection_log.json"), "w") as f:
        json.dump(summary, f, indent=2)

    print(json.dumps(summary, indent=2))
    print(f"Selesai dalam {summary['elapsed_sec']} detik -> {os.path.abspath(a.out_dir)}")


if __name__ == "__main__":
    main()
