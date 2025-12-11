#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi tabel: transaksi_lab_detail (source → target)
Struktur berbeda → mapping custom.
Kolom baru pada target:
    - id_duplo_detail = NULL
    - id_lab = NULL
    - satuan = NULL
    - id_asal = NULL
    - kode_hasil = "0"
"""

import os
import math
import sys
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# =====================================================
# 1. LOAD ENV
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


# =====================================================
# 2. ENGINE
# =====================================================
def make_engine(cfg):
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    )


src = make_engine(source_config)
tgt = make_engine(target_config)


print("===========================================================")
print("🚀 MIGRASI TABEL transaksi_lab_detail (CUSTOM + BATCH MODE)")
print("===========================================================\n")

print(f"📌 SOURCE DB : {source_config['database']}")
print(f"📌 TARGET DB : {target_config['database']}")
print(f"📌 Batch size: {BATCH_SIZE}\n")


# =====================================================
# 3. HITUNG TOTAL DATA
# =====================================================
with src.connect() as conn:
    total_rows = conn.execute(
        text("SELECT COUNT(*) FROM transaksi_lab_detail")).scalar()

print(f"📌 Total data di SOURCE : {total_rows}\n")


# =====================================================
# 4. SELECT BATCH SOURCE
# =====================================================
SELECT_BATCH = f"""
SELECT
    id_transaksi_lab_detail,
    id_transaksi_lab,
    id_kode_lab,
    kode_his,
    kode_tes,
    hasil,
    sebelum,
    cek_print,
    flag,
    rujukan,
    ket,
    acc,
    user_acc,
    validasi,
    user_validasi,
    alat,
    tgl_hasil,
    harga,
    waktu_sampel,
    kritis,
    manual,
    status_note,
    note_hasil,
    created_at,
    updated_at
FROM transaksi_lab_detail
ORDER BY id_transaksi_lab_detail
LIMIT {BATCH_SIZE} OFFSET :offset;
"""


# =====================================================
# 5. INSERT TARGET
# =====================================================
INSERT_SQL = """
INSERT INTO transaksi_lab_detail (
    id_transaksi_lab_detail,
    id_transaksi_lab,
    id_kode_lab,
    id_duplo_detail,
    kode_his,
    kode_tes,
    id_lab,
    kode_hasil,
    hasil,
    satuan,
    sebelum,
    cek_print,
    id_asal,
    flag,
    rujukan,
    ket,
    acc,
    user_acc,
    validasi,
    user_validasi,
    alat,
    tgl_hasil,
    harga,
    waktu_sampel,
    kritis,
    manual,
    status_note,
    note_hasil,
    created_at,
    updated_at
) VALUES (
    :id_transaksi_lab_detail,
    :id_transaksi_lab,
    :id_kode_lab,
    :id_duplo_detail,
    :kode_his,
    :kode_tes,
    :id_lab,
    :kode_hasil,
    :hasil,
    :satuan,
    :sebelum,
    :cek_print,
    :id_asal,
    :flag,
    :rujukan,
    :ket,
    :acc,
    :user_acc,
    :validasi,
    :user_validasi,
    :alat,
    :tgl_hasil,
    :harga,
    :waktu_sampel,
    :kritis,
    :manual,
    :status_note,
    :note_hasil,
    :created_at,
    :updated_at
)
"""


# =====================================================
# 6. PROGRESS BAR
# =====================================================
def progress_bar(cur, total):
    bar_len = 40
    filled = int(bar_len * cur / total) if total else bar_len
    bar = "█" * filled + "-" * (bar_len - filled)
    percent = (cur / total * 100) if total else 100
    sys.stdout.write(f"\r🔄 Progress: |{bar}| {percent:5.1f}%")
    sys.stdout.flush()


# =====================================================
# 7. MIGRASI
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
                r = dict(row._mapping)

                # === FIELD BARU ===
                r["id_duplo_detail"] = None
                r["id_lab"] = None
                r["satuan"] = None
                r["id_asal"] = None
                r["kode_hasil"] = "0"   # default

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

print("\n\n==============================================================")
print("🎉 MIGRASI transaksi_lab_detail SELESAI!")
print(f"✔ Total baris berhasil dimigrasikan: {inserted}")
print("⏱ Selesai pada:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("==============================================================\n")
