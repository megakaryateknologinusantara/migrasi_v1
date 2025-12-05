#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi tabel: kritis (source → target)
Struktur SAMA → langsung copy apa adanya.
Support:
    - Batch MIGRATION (MIG_BATCH_SIZE via .env)
    - Progress bar
"""

from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
import sys

# =====================================================
# 1. Load ENV
# =====================================================
load_dotenv()
BATCH_SIZE = int(os.getenv("MIG_BATCH_SIZE", "10000"))

# =====================================================
# 2. DB CONFIG
# =====================================================
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


print("==============================================")
print("🚀 MIGRASI TABEL kritis (BATCH MODE)")
print("==============================================\n")

print(f"📌 SOURCE DB : {source_config['database']}")
print(f"📌 TARGET DB : {target_config['database']}")
print(f"📌 Batch size: {BATCH_SIZE:,}\n")


# =====================================================
# 3. Hitung total baris
# =====================================================
with src.connect() as conn:
    total_rows = conn.execute(text("SELECT COUNT(*) FROM kritis")).scalar()

print(f"📌 Total data di SOURCE : {total_rows}\n")


# =====================================================
# 4. SELECT BATCH
# =====================================================
SELECT_BATCH = f"""
SELECT
    id,
    id_kode_lab,
    `case`,
    created_at,
    updated_at
FROM kritis
ORDER BY id
LIMIT {BATCH_SIZE} OFFSET :offset;
"""


# =====================================================
# 5. INSERT TARGET
# =====================================================
INSERT_SQL = """
INSERT INTO kritis (
    id,
    id_kode_lab,
    `case`,
    created_at,
    updated_at
) VALUES (
    :id,
    :id_kode_lab,
    :case,
    :created_at,
    :updated_at
)
"""


# =====================================================
# 6. Progress bar
# =====================================================
def progress_bar(cur, total):
    bar_len = 40
    fill = int(bar_len * cur / total)
    bar = "█" * fill + "-" * (bar_len - fill)
    percent = (cur / total * 100)
    sys.stdout.write(f"\r🔄 Progress: |{bar}| {percent:5.1f}%")
    sys.stdout.flush()


# =====================================================
# 7. MIGRASI
# =====================================================
offset = 0
total_inserted = 0

with tgt.connect() as conn_tgt:

    while True:

        with src.connect() as conn_src:
            rows = conn_src.execute(text(SELECT_BATCH), {
                                    "offset": offset}).fetchall()

        if not rows:
            break

        with conn_tgt.begin() as trans:
            try:
                for row in rows:
                    r = dict(row._mapping)
                    conn_tgt.execute(text(INSERT_SQL), r)

                total_inserted += len(rows)
                offset += BATCH_SIZE

                progress_bar(total_inserted, total_rows)

            except Exception as e:
                trans.rollback()
                print("\n❌ ERROR INSERT BATCH — ROLLBACK!")
                print("Error:", e)
                exit(1)

print("\n\n==============================================")
print("🎉 MIGRASI kritis SELESAI")
print("==============================================")
print(f"✔ Total inserted : {total_inserted}")
print("⏱ Selesai pada:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print("==============================================\n")
