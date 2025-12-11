#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi tabel: antibiotik (source → target)
Kolom di target memiliki tambahan: loinc
Menggunakan konfigurasi dari file .env
"""

from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os


# === [1] LOAD ENV FILE ===
load_dotenv()   # membaca file .env di directory yang sama


# === [2] KONFIG DATABASE via ENV ===
source_config = {
    'host': os.getenv('SRC_HOST'),
    'user': os.getenv('SRC_USER'),
    'password': os.getenv('SRC_PASSWORD'),
    'database': os.getenv('SRC_DATABASE'),
}

target_config = {
    'host': os.getenv('TGT_HOST'),
    'user': os.getenv('TGT_USER'),
    'password': os.getenv('TGT_PASSWORD'),
    'database': os.getenv('TGT_DATABASE'),
}


# === [3] Helper buat ENGINE ===
def make_engine(cfg):
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    )


src = make_engine(source_config)
tgt = make_engine(target_config)

print("🚀 Mulai migrasi tabel antibiotik...\n")
print(f"   SOURCE DB = {source_config['database']}")
print(f"   TARGET DB = {target_config['database']}\n")


# === [4] Ambil data dari SOURCE ===
select_sql = """
SELECT 
    id, 
    no_urut, 
    grub, 
    kode, 
    nama, 
    min, 
    max, 
    created_at, 
    updated_at
FROM antibiotik
ORDER BY id;
"""

with src.connect() as conn:
    result = conn.execute(text(select_sql))
    rows = result.fetchall()

print(f"📌 Total data ditemukan di SOURCE: {len(rows)}")


# === [5] Insert ke TARGET ===
insert_sql = """
INSERT INTO antibiotik (
    id,
    no_urut,
    grub,
    kode,
    nama,
    loinc,
    min,
    max,
    created_at,
    updated_at
) VALUES (
    :id,
    :no_urut,
    :grub,
    :kode,
    :nama,
    :loinc,
    :min,
    :max,
    :created_at,
    :updated_at
)
"""

count_ok = 0

with tgt.connect() as conn:
    trans = conn.begin()
    try:
        for row in rows:

            # SQLAlchemy Row mapping FIX
            data = dict(row._mapping)

            # kolom tambahan
            data["loinc"] = None

            conn.execute(text(insert_sql), data)
            count_ok += 1

        trans.commit()

    except Exception as e:
        trans.rollback()
        print("❌ ERROR migrasi, rollback dilakukan.")
        print("Error:", e)
        exit(1)


print("\n🎉 Migrasi selesai sukses!")
print(f"   ✔ Berhasil insert : {count_ok}")
print("⏱ Waktu selesai =", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
