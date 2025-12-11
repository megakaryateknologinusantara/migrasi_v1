#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi tabel: duplo_ori_detail (source → target)
Struktur sama → langsung copy 1:1
Support:
    - Batch MIGRATION (MIG_BATCH_SIZE via .env)
    - Progress bar
    - Rollback on error
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


print("======================================================")
print("🚀 MIGRASI TABEL duplo_ori_detail (BATCH MODE)")
print("======================================================\n")

print(f"📌 SOURCE DB : {source_config['database']}")
print(f"📌 TARGET DB : {target_config['database']}")
print(f"📌 Batch size: {BATCH_SIZE}\n")


# =====================================================
# 3. Hitung total rows di SOURCE
# =====================================================
with src.connect() as conn:
    total_rows = conn.execute(
        text("SELECT COUNT(*) FROM duplo_ori_detail")).scalar()

print(f"📌 Total data di SOURCE: {total_rows}\n")


# =====================================================
# 4. Query SELECT BATCH
# =====================================================
SELECT_BATCH = f"""
SELECT
    id_duplo_detail,
    id_duplo,
    kd_lis,
    hasil,
    satuan,
    nnormal,
    flag,
    date_run,
    created_at,
    updated_at
FROM duplo_ori_detail
ORDER BY id_duplo_detail
LIMIT {BATCH_SIZE} OFFSET :offset;
"""


# =====================================================
# 5. Query INSERT TARGET
# =====================================================
INSERT_SQL = """
INSERT INTO duplo_ori_detail (
    id_duplo_detail,
    id_duplo,
    kd_lis,
    hasil,
    satuan,
    nnormal,
    flag,
    date_run,
    created_at,
    updated_at
) VALUES (
    :id_duplo_detail,
    :id_duplo,
    :kd_lis,
    :hasil,
    :satuan,
    :nnormal,
    :flag,
    :date_run,
    :created_at,
    :updated_at
)
"""


# =====================================================
# 6. Progress Bar
# =====================================================
def progress_bar(cur, total):
    bar_len = 40
    fill = int(bar_len * cur / total) if total else bar_len
    bar = "█" * fill + "-" * (bar_len - fill)
    percent = (cur / total * 100) if total else 100
    sys.stdout.write(f"\r🔄 Progress: |{bar}| {percent:5.1f}%")
    sys.stdout.flush()


# =====================================================
# 7. MIGRASI
# =====================================================
offset = 0
total_inserted = 0

while True:

    with src.connect() as conn:
        rows = conn.execute(text(SELECT_BATCH), {"offset": offset}).fetchall()

    if not rows:
        break

    with tgt.connect() as conn:
        trans = conn.begin()
        try:
            for row in rows:
                conn.execute(text(INSERT_SQL), row._mapping)

            trans.commit()

        except Exception as e:
            trans.rollback()
            print("\n❌ ERROR INSERT BATCH — ROLLBACK!")
            print("Error:", e)
            exit(1)

    total_inserted += len(rows)
    offset += BATCH_SIZE
    progress_bar(total_inserted, total_rows)

print()


# =====================================================
# 8. FINISH
# =====================================================
print("\n🎉 MIGRASI duplo_ori_detail SELESAI!")
print(f"✔ Total baris berhasil dimigrasikan: {total_inserted}")
print("⏱ Selesai pada:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("======================================================\n")
