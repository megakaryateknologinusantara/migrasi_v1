#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi tabel: transaksi_paket_lab (source → target)
Struktur sama 100% → langsung copy.
"""

import os
import sys
import math
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# =====================================================
# 1. Load ENV
# =====================================================
load_dotenv()
BATCH_SIZE = int(os.getenv("MIG_BATCH_SIZE", "10000"))

source_config = {
    "host": os.getenv("SRC_HOST"),
    "user": os.getenv("SRC_USER"),
    "password": os.getenv("SRC_PASSWORD"),
    "database": os.getenv("SRC_DATABASE"),
}

target_config = {
    "host": os.getenv("TGT_HOST"),
    "user": os.getenv("TGT_USER"),
    "password": os.getenv("TGT_PASSWORD"),
    "database": os.getenv("TGT_DATABASE"),
}


def make_engine(cfg):
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    )


src = make_engine(source_config)
tgt = make_engine(target_config)

print("====================================================")
print("🚀 MIGRASI TABEL transaksi_paket_lab (BATCH MODE)")
print("====================================================\n")

print(f"📌 SOURCE DB : {source_config['database']}")
print(f"📌 TARGET DB : {target_config['database']}")
print(f"📌 Batch size: {BATCH_SIZE:,}\n")

# =====================================================
# 2. Hitung total data
# =====================================================
with src.connect() as conn:
    total_rows = conn.execute(
        text("SELECT COUNT(*) FROM transaksi_paket_lab")).scalar()

print(f"📌 Total data di SOURCE : {total_rows}\n")

# =====================================================
# 3. SELECT batch
# =====================================================
SELECT_BATCH = f"""
SELECT
    id_transaksi_paket_lab,
    id_transaksi_lab,
    id_paket_lab,
    waktu,
    cito,
    id_harga_kultur,
    harga,
    created_at,
    updated_at
FROM transaksi_paket_lab
ORDER BY id_transaksi_paket_lab
LIMIT {BATCH_SIZE} OFFSET :offset;
"""

# =====================================================
# 4. INSERT
# =====================================================
INSERT_SQL = """
INSERT INTO transaksi_paket_lab (
    id_transaksi_paket_lab,
    id_transaksi_lab,
    id_paket_lab,
    waktu,
    cito,
    id_harga_kultur,
    harga,
    created_at,
    updated_at
) VALUES (
    :id_transaksi_paket_lab,
    :id_transaksi_lab,
    :id_paket_lab,
    :waktu,
    :cito,
    :id_harga_kultur,
    :harga,
    :created_at,
    :updated_at
)
"""

# =====================================================
# 5. Progress bar
# =====================================================


def progress_bar(cur, total):
    bar_len = 40
    fill = int(bar_len * cur / total)
    bar = "█" * fill + "-" * (bar_len - fill)
    percent = (cur / total * 100)
    sys.stdout.write(f"\r🔄 Progress: |{bar}| {percent:5.1f}%")
    sys.stdout.flush()


# =====================================================
# 6. MIGRASI
# =====================================================
offset = 0
inserted = 0

print("🔄 Memulai migrasi ...\n")

while True:
    with src.connect() as conn:
        rows = conn.execute(text(SELECT_BATCH), {"offset": offset}).fetchall()

    if not rows:
        break

    with tgt.connect() as conn:
        trans = conn.begin()
        try:
            for row in rows:
                r = dict(row._mapping)
                conn.execute(text(INSERT_SQL), r)
                inserted += 1
                progress_bar(inserted, total_rows)
            trans.commit()

        except Exception as e:
            trans.rollback()
            print("\n❌ ERROR INSERT BATCH — ROLLBACK!")
            print("Error:", e)
            exit(1)

    offset += BATCH_SIZE

print("\n\n🎉 MIGRASI transaksi_paket_lab SELESAI")
print(f"✔ Total inserted : {inserted}")
print("⏱ Selesai pada :", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("====================================================\n")
