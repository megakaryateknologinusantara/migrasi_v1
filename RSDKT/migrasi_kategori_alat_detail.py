#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi tabel: kategori_alat_detail (source → target)
Perbedaan:
 - Source tidak memiliki kolom 'alias' → isi NULL
"""

import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime
import math

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

print("========================================================")
print("🚀 MIGRASI TABEL kategori_alat_detail (BATCH MODE)")
print("========================================================\n")

print(f"📌 SOURCE DB : {source_config['database']}")
print(f"📌 TARGET DB : {target_config['database']}")
print(f"📌 Batch size: {BATCH_SIZE}\n")

# =====================================================
# 2. Hitung total data source
# =====================================================
with src.connect() as conn:
    total_rows = conn.execute(
        text("SELECT COUNT(*) FROM kategori_alat_detail")).scalar()

print(f"📌 Total data di SOURCE : {total_rows}\n")

# =====================================================
# 3. SELECT batch
# =====================================================
SELECT_SQL = f"""
SELECT
    id_kategori_alat_detail,
    id_kategori_alat,
    id_kode_lab,
    no_item,
    kode_lis,
    kali,
    created_at,
    updated_at
FROM kategori_alat_detail
ORDER BY id_kategori_alat_detail
LIMIT {BATCH_SIZE} OFFSET :offset;
"""

# =====================================================
# 4. INSERT target
# =====================================================
INSERT_SQL = """
INSERT INTO kategori_alat_detail (
    id_kategori_alat_detail,
    id_kategori_alat,
    id_kode_lab,
    no_item,
    kode_lis,
    alias,
    kali,
    created_at,
    updated_at
) VALUES (
    :id_kategori_alat_detail,
    :id_kategori_alat,
    :id_kode_lab,
    :no_item,
    :kode_lis,
    :alias,
    :kali,
    :created_at,
    :updated_at
)
"""

# =====================================================
# 5. Simple progress bar
# =====================================================


def progress(cur, total):
    size = 40
    filled = int(size * cur / total) if total else size
    bar = "█" * filled + "-" * (size - filled)
    percent = (cur / total * 100) if total else 100
    sys.stdout.write(f"\r🔄 Progress: |{bar}| {percent:5.1f}%")
    sys.stdout.flush()


# =====================================================
# 6. Migrasi batch
# =====================================================
offset = 0
inserted = 0

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
                    "id_kategori_alat_detail": r["id_kategori_alat_detail"],
                    "id_kategori_alat": r["id_kategori_alat"],
                    "id_kode_lab": r["id_kode_lab"],
                    "no_item": r["no_item"],
                    "kode_lis": r["kode_lis"],
                    "alias": None,       # field tidak ada di source
                    "kali": r["kali"],
                    "created_at": r["created_at"],
                    "updated_at": r["updated_at"],
                }

                conn.execute(text(INSERT_SQL), data)
                inserted += 1
                progress(inserted, total_rows)

            trans.commit()

        except Exception as e:
            trans.rollback()
            print("\n❌ ERROR! Batch rollback dilakukan!")
            print("Error:", e)
            exit(1)

    offset += BATCH_SIZE

print("\n\n========================================================")
print("🎉 MIGRASI kategori_alat_detail SELESAI")
print("========================================================")
print(f"✔ Total inserted : {inserted}")
print("⏱ Selesai pada   :", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("========================================================\n")
