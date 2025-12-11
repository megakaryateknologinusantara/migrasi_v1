#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi tabel: duplo_detail (source → target)
Perbedaan struktur:
    - Tabel lama TIDAK memiliki kolom 'periksa'
      → Pada tabel target, kolom 'periksa' diisi NULL
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

BATCH_SIZE = int(os.getenv("MIG_BATCH_SIZE", "10000"))


def make_engine(cfg):
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    )


src = make_engine(source_config)
tgt = make_engine(target_config)


print("===============================================")
print("🚀 MIGRASI TABEL duplo_detail (BATCH MODE)")
print("===============================================\n")

print(f"📌 SOURCE DB : {source_config['database']}")
print(f"📌 TARGET DB : {target_config['database']}")
print(f"📌 Batch size: {BATCH_SIZE:,}\n")


# =====================================================
# 2. Hitung total rows
# =====================================================
with src.connect() as conn:
    total_rows = conn.execute(
        text("SELECT COUNT(*) FROM duplo_detail")).scalar()

print(f"📌 Total data di SOURCE: {total_rows}\n")


# =====================================================
# 3. Select batch
# =====================================================
SELECT_SQL = f"""
SELECT
    id_duplo_detail,
    id_duplo,
    kd_lis,
    hasil,
    satuan,
    nnormal,
    flag,
    datetime_sample,
    created_at,
    updated_at
FROM duplo_detail
ORDER BY id_duplo_detail
LIMIT {BATCH_SIZE} OFFSET :offset;
"""


# =====================================================
# 4. INSERT SQL ke TARGET
# =====================================================
INSERT_SQL = """
INSERT INTO duplo_detail (
    id_duplo_detail,
    id_duplo,
    kd_lis,
    periksa,
    hasil,
    satuan,
    nnormal,
    flag,
    datetime_sample,
    created_at,
    updated_at
) VALUES (
    :id_duplo_detail,
    :id_duplo,
    :kd_lis,
    :periksa,
    :hasil,
    :satuan,
    :nnormal,
    :flag,
    :datetime_sample,
    :created_at,
    :updated_at
)
"""


# =====================================================
# 5. Progress bar
# =====================================================
def progress_bar(cur, total):
    bar_len = 40
    filled = int(bar_len * cur / total)
    bar = "█" * filled + "-" * (bar_len - filled)
    percent = (cur / total * 100)
    sys.stdout.write(f"\r🔄 Progress: |{bar}| {percent:5.1f}%")
    sys.stdout.flush()


# =====================================================
# 6. Eksekusi batch insert
# =====================================================
offset = 0
inserted = 0
total_batches = math.ceil(total_rows / BATCH_SIZE)

print("🔄 Memulai migrasi ...\n")

while True:

    with src.connect() as conn:
        rows = conn.execute(text(SELECT_SQL), {"offset": offset}).fetchall()

    if not rows:
        break

    with tgt.connect() as conn:
        trans = conn.begin()
        try:
            for row in rows:
                r = row._mapping

                data = {
                    "id_duplo_detail": r["id_duplo_detail"],
                    "id_duplo": r["id_duplo"],
                    "kd_lis": r["kd_lis"],
                    "periksa": None,  # kolom baru — isi NULL
                    "hasil": r["hasil"],
                    "satuan": r["satuan"],
                    "nnormal": r["nnormal"],
                    "flag": r["flag"],
                    "datetime_sample": r["datetime_sample"],
                    "created_at": r["created_at"],
                    "updated_at": r["updated_at"],
                }

                conn.execute(text(INSERT_SQL), data)
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
print("🎉 MIGRASI duplo_detail SELESAI")
print("===============================================")
print(f"✔ Total inserted : {inserted}")
print(f"⏱ Selesai pada   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("===============================================\n")
