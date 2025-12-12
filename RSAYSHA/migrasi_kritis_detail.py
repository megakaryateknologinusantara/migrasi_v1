#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi tabel: kritis_detail (source → target)
Struktur hampir sama, hanya target memiliki kolom 'single'.
Isi default:
    - single = NULL
ID dari source dibawa ke target (tidak auto increment).
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

print("===============================================")
print("🚀 MIGRASI TABEL kritis_detail (BATCH MODE)")
print("===============================================\n")

print(f"📌 SOURCE DB : {source_config['database']}")
print(f"📌 TARGET DB : {target_config['database']}")
print(f"📌 Batch size: {BATCH_SIZE:,}\n")

# =====================================================
# 2. Hitung total data source
# =====================================================
with src.connect() as conn:
    total_rows = conn.execute(
        text("SELECT COUNT(*) FROM kritis_detail")).scalar()

print(f"📌 Total data di SOURCE : {total_rows}\n")

# =====================================================
# 3. SELECT batch
# =====================================================
SELECT_BATCH = f"""
SELECT
    id,
    id_kritis,
    `case`,
    gender,
    umur1,
    rangeu,
    umur2,
    waktu,
    nr1,
    rangen,
    nr2,
    nrujukan,
    created_at,
    updated_at
FROM kritis_detail
ORDER BY id
LIMIT {BATCH_SIZE} OFFSET :offset;
"""

# =====================================================
# 4. INSERT ke target
# =====================================================
INSERT_SQL = """
INSERT INTO kritis_detail (
    id,
    id_kritis,
    `case`,
    gender,
    umur1,
    rangeu,
    umur2,
    waktu,
    nr1,
    rangen,
    nr2,
    nrujukan,
    single,
    created_at,
    updated_at
) VALUES (
    :id,
    :id_kritis,
    :case,
    :gender,
    :umur1,
    :rangeu,
    :umur2,
    :waktu,
    :nr1,
    :rangen,
    :nr2,
    :nrujukan,
    :single,
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
# 6. Proses migrasi
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

                # Kolom tambahan
                r["single"] = None

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

print("\n\n===============================================")
print("🎉 MIGRASI kritis_detail SELESAI")
print("===============================================")
print(f"✔ Total inserted : {inserted}")
print(f"⏱ Selesai pada   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("===============================================\n")
