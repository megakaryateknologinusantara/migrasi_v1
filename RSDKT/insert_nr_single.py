#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi data dari tabel kode_lab → kode_lab_detail (target → target)
Aturan:
 - Hanya mengambil row kode_lab WHERE case = '4'
 - id_kode_lab_detail → AUTO_INCREMENT (tidak dimasukkan)
 - Field mapping sesuai instruksi user
 - Batch mode menggunakan MIG_BATCH_SIZE dari .env
"""

import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime
import math

# ==============================================================
# 1. Load ENV
# ==============================================================
load_dotenv()

BATCH_SIZE = int(os.getenv("MIG_BATCH_SIZE", "10000"))

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


tgt = make_engine(target_config)

print("==========================================================")
print("🚀 MIGRASI kode_lab → kode_lab_detail (TARGET → TARGET)")
print("==========================================================\n")

print(f"📌 DATABASE : {target_config['database']}")
print(f"📌 Batch size: {BATCH_SIZE:,}\n")


# ==============================================================
# 2. Hitung total row kode_lab WHERE case = '4'
# ==============================================================
with tgt.connect() as conn:
    total_rows = conn.execute(
        text("SELECT COUNT(*) FROM kode_lab WHERE `case` = '4'")
    ).scalar()

print(f"📌 Total data yang akan dimigrasi: {total_rows}\n")

if total_rows == 0:
    print("Tidak ada data dengan case='4'. Stop proses.")
    exit(0)


# ==============================================================
# 3. Query SELECT Batch
# ==============================================================
SELECT_BATCH = f"""
SELECT
    id_kode_lab,
    nilai_rujukan,
    `case`,
    created_at,
    updated_at
FROM kode_lab
WHERE `case` = '4'
ORDER BY id_kode_lab
LIMIT {BATCH_SIZE} OFFSET :offset;
"""


# ==============================================================
# 4. Query Insert (tanpa id_kode_lab_detail → AUTO_INCREMENT)
# ==============================================================
INSERT_SQL = """
INSERT INTO kode_lab_detail (
    id_kode_lab,
    urut,
    ket,
    `case`,
    sex,
    umur1,
    rangeu,
    umur2,
    waktu,
    nr1,
    rangen,
    nr2,
    single,
    nrujukan,
    created_at,
    updated_at
) VALUES (
    :id_kode_lab,
    :urut,
    :ket,
    :case,
    :sex,
    :umur1,
    :rangeu,
    :umur2,
    :waktu,
    :nr1,
    :rangen,
    :nr2,
    :single,
    :nrujukan,
    :created_at,
    :updated_at
)
"""


# ==============================================================
# 5. Progress Bar
# ==============================================================
def progress_bar(cur, total):
    bar_len = 40
    fill = int(bar_len * cur / total) if total else bar_len
    bar = "█" * fill + "-" * (bar_len - fill)
    pct = (cur / total * 100) if total else 100
    sys.stdout.write(f"\r🔄 Progress: |{bar}| {pct:5.1f}%")
    sys.stdout.flush()


# ==============================================================
# 6. Batch Loop
# ==============================================================
offset = 0
inserted = 0

while True:

    # Ambil batch
    with tgt.connect() as conn:
        rows = conn.execute(text(SELECT_BATCH), {"offset": offset}).fetchall()

    if not rows:
        break

    # Insert batch
    with tgt.connect() as conn:
        trans = conn.begin()
        try:
            for row in rows:
                r = row._mapping

                nilai = r["nilai_rujukan"]

                data = {
                    "id_kode_lab": r["id_kode_lab"],
                    "urut": None,
                    "ket": None,
                    "case": r["case"],
                    "sex": "0",
                    "umur1": 0.0000,
                    "rangeu": None,
                    "umur2": 0.0000,
                    "waktu": "0",
                    "nr1": 0.0000,
                    "rangen": "0",
                    "nr2": 0.0000,
                    "single": nilai,
                    "nrujukan": nilai,
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

print("\n\n🎉 MIGRASI SELESAI!")
print(f"✔ Total baris dimigrasi: {inserted}")
print("⏱ Waktu selesai :", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("==========================================================\n")
