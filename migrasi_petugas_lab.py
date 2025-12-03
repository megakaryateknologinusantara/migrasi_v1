#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi tabel: petugas_lab (source → target)
Fitur:
    - Batch processing (default 10.000 dari .env)
    - Progress bar
    - Struktur kedua tabel sama persis
"""

from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
import sys


# === [1] Load .env ===
load_dotenv()

BATCH_SIZE = int(os.getenv("MIG_BATCH_SIZE", "10000"))


# === [2] Konfigurasi DB dari ENV ===
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


# === [3] Engine helper ===
def make_engine(cfg):
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    )


src = make_engine(source_config)
tgt = make_engine(target_config)


print("=====================================================")
print("🚀 MIGRASI TABEL petugas_lab (BATCH + PROGRESS BAR)")
print("=====================================================\n")

print(f"📌 SOURCE DB : {source_config['database']}")
print(f"📌 TARGET DB : {target_config['database']}")
print(f"📌 Batch size: {BATCH_SIZE}\n")


# === [4] Hitung total row ===
with src.connect() as conn:
    total_rows = conn.execute(
        text("SELECT COUNT(*) FROM petugas_lab")).scalar()

print(f"📌 Total data di SOURCE: {total_rows}\n")


# === [5] Query SELECT batch ===
SELECT_BATCH = f"""
SELECT
    id_petugas_lab,
    nama,
    created_at,
    updated_at
FROM petugas_lab
ORDER BY id_petugas_lab
LIMIT {BATCH_SIZE} OFFSET :offset;
"""


# === [6] Query INSERT target ===
INSERT_SQL = """
INSERT INTO petugas_lab (
    id_petugas_lab,
    nama,
    created_at,
    updated_at
) VALUES (
    :id_petugas_lab,
    :nama,
    :created_at,
    :updated_at
)
"""


# === [7] Progress bar ===
def progress_bar(current, total):
    bar_len = 40
    filled = int(bar_len * current / total) if total > 0 else 0
    bar = "█" * filled + "-" * (bar_len - filled)
    percent = (current / total * 100) if total > 0 else 100
    sys.stdout.write(f"\r🔄 Progress: |{bar}| {percent:5.1f}%")
    sys.stdout.flush()


# === [8] Migrasi per batch ===
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
            print("\n❌ ERROR batch, rollback dilakukan!")
            print("Error:", e)
            exit(1)

    total_inserted += len(rows)
    offset += BATCH_SIZE

    progress_bar(total_inserted, total_rows)

print()  # newline


print("\n🎉 MIGRASI SELESAI!")
print(f"✔ Total petugas_lab dimigrasikan: {total_inserted}")
print("⏱ Waktu selesai =", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("=====================================================\n")
