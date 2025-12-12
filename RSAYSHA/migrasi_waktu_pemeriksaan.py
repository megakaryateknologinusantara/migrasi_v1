#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi tabel: waktu_pemeriksaan (source → target)
Struktur kedua tabel sama.
Support:
    - Batch MIGRATION (MIG_BATCH_SIZE via .env)
    - Progress bar
"""

from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
import sys


# === [1] Load ENV ===
load_dotenv()
BATCH_SIZE = int(os.getenv("MIG_BATCH_SIZE", "10000"))


# === [2] Database config ===
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


# === [3] Engine builder ===
def make_engine(cfg):
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    )


src = make_engine(source_config)
tgt = make_engine(target_config)


print("=================================================")
print("🚀 MIGRASI TABEL waktu_pemeriksaan (BATCH + PROGRESS BAR)")
print("=================================================\n")

print(f"📌 SOURCE DB : {source_config['database']}")
print(f"📌 TARGET DB : {target_config['database']}")
print(f"📌 Batch size: {BATCH_SIZE}\n")


# === [4] Hitung total row ===
with src.connect() as conn:
    total_rows = conn.execute(
        text("SELECT COUNT(*) FROM waktu_pemeriksaan")).scalar()

print(f"📌 Total data di SOURCE: {total_rows}\n")


# === [5] Query SELECT per batch ===
SELECT_BATCH = f"""
SELECT
    id,
    id_paket,
    status,
    waktu,
    cito,
    created_at,
    updated_at,
    grub
FROM waktu_pemeriksaan
ORDER BY id
LIMIT {BATCH_SIZE} OFFSET :offset;
"""


# === [6] Query INSERT ===
INSERT_SQL = """
INSERT INTO waktu_pemeriksaan (
    id,
    id_paket,
    status,
    waktu,
    cito,
    created_at,
    updated_at,
    grub
) VALUES (
    :id,
    :id_paket,
    :status,
    :waktu,
    :cito,
    :created_at,
    :updated_at,
    :grub
)
"""


# === [7] Progress bar ===
def progress_bar(current, total):
    bar_len = 40
    filled = int(bar_len * current / total) if total > 0 else 40
    bar = "█" * filled + "-" * (bar_len - filled)
    percent = (current / total * 100) if total > 0 else 100
    sys.stdout.write(f"\r🔄 Progress: |{bar}| {percent:5.1f}%")
    sys.stdout.flush()


# === [8] MIGRASI BATCH ===
offset = 0
total_inserted = 0

while True:
    # Ambil batch dari source
    with src.connect() as conn:
        rows = conn.execute(text(SELECT_BATCH), {"offset": offset}).fetchall()

    if not rows:
        break

    # Insert batch ke target
    with tgt.connect() as conn:
        trans = conn.begin()
        try:
            for row in rows:
                conn.execute(text(INSERT_SQL), dict(row._mapping))
            trans.commit()
        except Exception as e:
            trans.rollback()
            print("\n❌ ERROR saat insert batch, rollback dilakukan!")
            print("Error:", e)
            exit(1)

    total_inserted += len(rows)
    offset += BATCH_SIZE
    progress_bar(total_inserted, total_rows)

print()


# === [9] Selesai ===
print("\n🎉 MIGRASI waktu_pemeriksaan SELESAI!")
print(f"✔ Total baris dimigrasikan: {total_inserted}")
print("⏱ Waktu selesai =", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("=================================================\n")
