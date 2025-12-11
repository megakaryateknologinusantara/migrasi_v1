#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi tabel: kategori_alat (source → target)
Struktur berbeda → mapping custom.
Field tambahan target:
 - grub_alat = NULL
 - status = 0
"""

import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime

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


# =====================================================
# 2. Engine
# =====================================================
def make_engine(cfg):
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    )


src = make_engine(source_config)
tgt = make_engine(target_config)


print("====================================================")
print("🚀 MIGRASI TABEL kategori_alat (CUSTOM + BATCH MODE)")
print("====================================================\n")

print(f"📌 SOURCE DB : {source_config['database']}")
print(f"📌 TARGET DB : {target_config['database']}")
print(f"📌 Batch size: {BATCH_SIZE}\n")


# =====================================================
# 3. Hitung total data
# =====================================================
with src.connect() as conn:
    total_rows = conn.execute(
        text("SELECT COUNT(*) FROM kategori_alat")).scalar()

print(f"📌 Total data di SOURCE: {total_rows}\n")


# =====================================================
# 4. SELECT batch
# =====================================================
SELECT_BATCH = f"""
SELECT
    id_kategori_alat,
    nama,
    kode_lis,
    sn,
    id_user,
    created_at,
    updated_at
FROM kategori_alat
ORDER BY id_kategori_alat
LIMIT {BATCH_SIZE} OFFSET :offset;
"""


# =====================================================
# 5. INSERT target
# =====================================================
INSERT_SQL = """
INSERT INTO kategori_alat (
    id_kategori_alat,
    nama,
    kode_lis,
    SN,
    grub_alat,
    id_user,
    status,
    created_at,
    updated_at
) VALUES (
    :id_kategori_alat,
    :nama,
    :kode_lis,
    :SN,
    :grub_alat,
    :id_user,
    :status,
    :created_at,
    :updated_at
)
"""


# =====================================================
# 6. Progress bar
# =====================================================
def progress(cur, total):
    bar_len = 40
    filled = int(bar_len * cur / total) if total else bar_len
    bar = "█" * filled + "-" * (bar_len - filled)
    percent = (cur / total * 100) if total else 100
    sys.stdout.write(f"\r🔄 Progress: |{bar}| {percent:5.1f}%")
    sys.stdout.flush()


# =====================================================
# 7. Eksekusi migrasi
# =====================================================
offset = 0
inserted = 0

while True:

    with src.connect() as conn:
        rows = conn.execute(text(SELECT_BATCH), {"offset": offset}).fetchall()

    if not rows:
        break

    with tgt.connect() as conn:
        trans = conn.begin()

        try:
            for row in rows:
                r = row._mapping

                data = {
                    "id_kategori_alat": r["id_kategori_alat"],
                    "nama": r["nama"],
                    "kode_lis": r["kode_lis"],
                    "SN": r["sn"],            # lowercase di source
                    "grub_alat": None,        # tidak ada di source
                    "id_user": r["id_user"],
                    "status": 0,              # default
                    "created_at": r["created_at"],
                    "updated_at": r["updated_at"],
                }

                conn.execute(text(INSERT_SQL), data)
                inserted += 1
                progress(inserted, total_rows)

            trans.commit()

        except Exception as e:
            trans.rollback()
            print("\n❌ ERROR saat insert batch. ROLLBACK!")
            print("Error:", e)
            exit(1)

    offset += BATCH_SIZE

print("\n\n🎉 MIGRASI kategori_alat SELESAI!")
print(f"✔ Total inserted: {inserted}")
print("⏱ Selesai pada:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("====================================================\n")
