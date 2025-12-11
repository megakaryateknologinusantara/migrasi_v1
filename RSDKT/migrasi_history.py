#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi tabel: history (source → target)
FITUR:
    - Batching (default 10.000 row per batch, bisa ganti via .env)
    - Progress bar akurat
    - Menggunakan konfigurasi dari .env
"""

from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
import sys


# === [1] LOAD ENV ===
load_dotenv()

BATCH_SIZE = int(os.getenv("MIG_BATCH_SIZE", "10000"))


# === [2] CONFIG DATABASE ===
source_config = {
    'host': os.getenv('SRC_HOST'),
    'user': os.getenv('SRC_USER'),
    'password': os.getenv('SRC_PASSWORD'),
    'database': os.getenv('SRC_DATABASE'),
}

target_config = {
    'host': os.getenv('TGT_HOST'),
    'user': os.getenv('TGT_USER'),
    'password': os.getenv('TGT_PASSWORD'),
    'database': os.getenv('TGT_DATABASE'),
}


# === [3] ENGINE ===
def make_engine(cfg):
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    )


src = make_engine(source_config)
tgt = make_engine(target_config)

print("🚀 Migrasi tabel history dengan batching & progress bar")
print(f"   Batch size = {BATCH_SIZE}\n")


# === [4] Hitung total data ===
with src.connect() as conn:
    total_rows = conn.execute(text("SELECT COUNT(*) FROM history")).scalar()

print(f"📌 Total row sumber: {total_rows}\n")


# === [5] QUERY SELECT per batch ===
SELECT_BATCH = f"""
SELECT
    id,
    id_transaksi_lab,
    id_user,
    aktivitas,
    keterangan,
    created_at,
    updated_at
FROM history
ORDER BY id
LIMIT {BATCH_SIZE} OFFSET :offset;
"""

# === [6] QUERY INSERT ===
INSERT_SQL = """
INSERT INTO history (
    id,
    id_transaksi_lab,
    id_user,
    aktivitas,
    keterangan,
    created_at,
    updated_at
) VALUES (
    :id,
    :id_transaksi_lab,
    :id_user,
    :aktivitas,
    :keterangan,
    :created_at,
    :updated_at
)
"""


# === Helper: Progress Bar ===
def progress_bar(progress, total):
    bar_length = 40  # panjang bar
    filled = int(bar_length * progress / total)
    bar = "█" * filled + "-" * (bar_length - filled)
    percent = (progress / total) * 100
    sys.stdout.write(f"\r🔄 Progress: |{bar}| {percent:5.1f}%")
    sys.stdout.flush()


# === [7] PROSES PER BATCH ===
total_inserted = 0
offset = 0

while True:
    with src.connect() as conn:
        rows = conn.execute(text(SELECT_BATCH), {"offset": offset}).fetchall()

    if not rows:
        break  # selesai

    with tgt.connect() as conn:
        trans = conn.begin()
        try:
            for row in rows:
                conn.execute(text(INSERT_SQL), dict(row._mapping))
            trans.commit()
        except Exception as e:
            trans.rollback()
            print("\n❌ ERROR batch, rollback dilakukan!")
            print(e)
            exit(1)

    total_inserted += len(rows)
    offset += BATCH_SIZE

    # Update progress bar
    progress_bar(total_inserted, total_rows)

# newline setelah progress bar
print()

print("\n🎉 MIGRASI SELESAI TOTAL!")
print(f"✔ Total data inserted: {total_inserted}")
print("⏱ Waktu selesai =", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
