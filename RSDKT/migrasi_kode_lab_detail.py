#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi tabel: kode_lab_detail (source → target)
Aturan khusus:
 - Kolom tambahan di target: urut=NULL, single=NULL
 - Mapping nilai rangen:
      0→0, 4→3, 5→4, 6→1, 7→2
 - Jika rangen tidak cocok mapping → row SKIP
"""

import os
import math
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime
import sys

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


# =====================================================
# 2. Create Engine
# =====================================================
def make_engine(cfg):
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    )


src = make_engine(source_config)
tgt = make_engine(target_config)

print("==============================================")
print("🚀 MIGRASI TABEL kode_lab_detail (BATCH MODE)")
print("==============================================\n")

print(f"📌 SOURCE DB : {source_config['database']}")
print(f"📌 TARGET DB : {target_config['database']}")
print(f"📌 Batch size: {BATCH_SIZE:,}\n")


# =====================================================
# 3. Ambil data dari SOURCE
# =====================================================
select_sql = """
SELECT
    id_kode_lab_detail,
    id_kode_lab,
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
    nrujukan,
    created_at,
    updated_at
FROM kode_lab_detail
ORDER BY id_kode_lab_detail;
"""

with src.connect() as conn:
    rows = conn.execute(text(select_sql)).fetchall()

total_rows = len(rows)
print(f"📌 Total data di SOURCE : {total_rows}\n")


# =====================================================
# 4. Mapping rangen sesuai aturan
# =====================================================
RANGEN_MAP = {
    0: 0,
    4: 3,
    5: 4,
    6: 1,
    7: 2,
}


# =====================================================
# 5. Query INSERT ke TARGET
# =====================================================
insert_sql = """
INSERT INTO kode_lab_detail (
    id_kode_lab_detail,
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
    :id_kode_lab_detail,
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


# =====================================================
# 6. Progress Bar Manual
# =====================================================
def progress_bar(current, total):
    bar_len = 40
    filled = int(bar_len * current / total)
    bar = "█" * filled + "-" * (bar_len - filled)
    percent = (current / total * 100)
    sys.stdout.write(f"\r🔄 Progress: |{bar}| {percent:5.1f}%")
    sys.stdout.flush()


# =====================================================
# 7. Batch Insert
# =====================================================
total_batches = math.ceil(total_rows / BATCH_SIZE)
inserted = 0
skipped = 0

print("🔄 Memulai migrasi ...\n")

with tgt.connect() as conn:

    for batch_no in range(total_batches):

        start = batch_no * BATCH_SIZE
        end = min(start + BATCH_SIZE, total_rows)
        batch = rows[start:end]

        with conn.begin():

            for row in batch:

                r = row._mapping

                # Convert rangen ke integer
                try:
                    old_rangen = int(r["rangen"])
                except:
                    skipped += 1
                    continue

                # Skip jika tidak masuk mapping
                if old_rangen not in RANGEN_MAP:
                    skipped += 1
                    continue

                # Mapping nilai baru
                new_rangen = RANGEN_MAP[old_rangen]

                data = {
                    "id_kode_lab_detail": r["id_kode_lab_detail"],
                    "id_kode_lab": r["id_kode_lab"],
                    "urut": None,          # kolom tambahan → NULL
                    "ket": r["ket"],
                    "case": r["case"],
                    "sex": r["sex"],
                    "umur1": r["umur1"],
                    "rangeu": r["rangeu"],
                    "umur2": r["umur2"],
                    "waktu": r["waktu"],
                    "nr1": r["nr1"],
                    "rangen": new_rangen,
                    "nr2": r["nr2"],
                    "single": None,        # kolom tambahan → NULL
                    "nrujukan": r["nrujukan"],
                    "created_at": r["created_at"],
                    "updated_at": r["updated_at"],
                }

                conn.execute(text(insert_sql), data)
                inserted += 1
                progress_bar(inserted + skipped, total_rows)

print("\n\n==============================================")
print("🎉 MIGRASI kode_lab_detail SELESAI")
print("==============================================")
print(f"✔ Total inserted : {inserted}")
print(f"⚠ Total skipped  : {skipped}")
print(f"⏱ Selesai pada   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("==============================================\n")
