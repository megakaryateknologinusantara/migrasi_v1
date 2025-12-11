#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi tabel: kode_lab (source → target)
Struktur berbeda → mapping custom.
Tambahan khusus:
    - kolom lama kode_his → isi juga ke kolom MIN pada tabel target
Support:
    - Batch MIGRATION (MIG_BATCH_SIZE via .env)
    - Progress bar
    - Penambahan field baru secara default
    - Konversi tipe_hasil (int → str)
    - Logic nilai_rujukan menentukan field 'case'
"""

from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
import sys


# === [1] Load ENV ===
load_dotenv()
BATCH_SIZE = int(os.getenv("MIG_BATCH_SIZE", "10000"))


# === [2] DB CONFIG ===
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
print("🚀 MIGRASI TABEL kode_lab (CUSTOM FIELD + BATCH MODE)")
print("======================================================\n")

print(f"📌 SOURCE DB : {source_config['database']}")
print(f"📌 TARGET DB : {target_config['database']}")
print(f"📌 Batch size: {BATCH_SIZE}\n")


# === [3] Hitung total rows ===
with src.connect() as conn:
    total_rows = conn.execute(text("SELECT COUNT(*) FROM kode_lab")).scalar()

print(f"📌 Total data di SOURCE: {total_rows}\n")


# === [4] SELECT BATCH ===
SELECT_BATCH = f"""
SELECT
    id_kode_lab,
    id_sub_kategori,
    grub1,
    grub2,
    grub3,
    kodelab_global,
    nama,
    kode_tes,
    kd_lis,
    satuan,
    nilai_rujukan,
    metoda,
    `case`,
    kode_his,
    harga,
    keterangan,
    info,
    koma,
    min,
    max,
    tipe_hasil,
    metode_input,
    created_at,
    updated_at
FROM kode_lab
ORDER BY id_kode_lab
LIMIT {BATCH_SIZE} OFFSET :offset;
"""


# === [5] INSERT TARGET ===
INSERT_SQL = """
INSERT INTO kode_lab (
    id_kode_lab,
    id_spesimen_snomed,
    id_sub_kategori,
    grub1,
    grub2,
    grub3,
    kodelab_global,
    nama,
    en,
    kode_loinc,
    loinc_satuan,
    type_loinc,
    code_system,
    kode_tes,
    kd_lis,
    satuan,
    status,
    nilai_rujukan,
    metoda,
    `case`,
    kode_his,
    harga,
    keterangan,
    info,
    koma,
    min,
    max,
    tipe_hasil,
    flaging,
    metode_input,
    created_at,
    updated_at,
    nilai_default
) VALUES (
    :id_kode_lab,
    :id_spesimen_snomed,
    :id_sub_kategori,
    :grub1,
    :grub2,
    :grub3,
    :kodelab_global,
    :nama,
    :en,
    :kode_loinc,
    :loinc_satuan,
    :type_loinc,
    :code_system,
    :kode_tes,
    :kd_lis,
    :satuan,
    :status,
    :nilai_rujukan,
    :metoda,
    :case_value,
    :kode_his,
    :harga,
    :keterangan,
    :info,
    :koma,
    :min,
    :max,
    :tipe_hasil,
    :flaging,
    :metode_input,
    :created_at,
    :updated_at,
    :nilai_default
)
"""


# === [6] Progress bar ===
def progress_bar(cur, total):
    bar_len = 40
    fill = int(bar_len * cur / total) if total else bar_len
    bar = "█" * fill + "-" * (bar_len - fill)
    percent = (cur / total * 100) if total else 100
    sys.stdout.write(f"\r🔄 Progress: |{bar}| {percent:5.1f}%")
    sys.stdout.flush()


# === [7] MIGRASI ===
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

                r = dict(row._mapping)

                # === FIELD BARU DEFAULT ===
                r["id_spesimen_snomed"] = None
                r["en"] = None
                r["kode_loinc"] = None
                r["loinc_satuan"] = None
                r["type_loinc"] = None
                r["code_system"] = "http://loinc.org"
                r["status"] = "1"
                r["flaging"] = 1
                r["nilai_default"] = None

                # === KONVERSI tipe_hasil INT → STR ===
                r["tipe_hasil"] = str(
                    r["tipe_hasil"]) if r["tipe_hasil"] is not None else None

                # === LOGIC nilai_rujukan menentukan CASE ===
                if isinstance(r["nilai_rujukan"], str) and r["nilai_rujukan"].strip() == "-":
                    r["case_value"] = r["case"]   # pakai case lama
                else:
                    r["case_value"] = "4"         # set case = 4

                # === PERMINTAAN KHUSUS ===
                # kode_his → juga masuk ke kolom MIN target
                r["min"] = r["kode_his"] if r["kode_his"] else r["min"]

                conn.execute(text(INSERT_SQL), r)

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


# === [8] FINISH ===
print("\n🎉 MIGRASI kode_lab SELESAI!")
print(f"✔ Total baris berhasil dimigrasikan: {total_inserted}")
print("⏱ Selesai pada:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("======================================================\n")
