#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: cek_foreign_key.py
Deskripsi:
    - Mengambil semua foreign key dari database target (atau source)
    - Menampilkan tabel + jumlah FK
    - Menggunakan konfigurasi dari file .env
"""

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
import os


# === [1] LOAD ENV FILE ===
load_dotenv()


# === [2] KONFIG DATABASE TARGET ===
target_config = {
    'host': os.getenv('TGT_HOST'),
    'user': os.getenv('TGT_USER'),
    'password': os.getenv('TGT_PASSWORD'),
    'database': os.getenv('TGT_DATABASE'),
}


# === [3] Buat ENGINE ===
def create_mysql_engine(cfg):
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    )


tgt_engine = create_mysql_engine(target_config)
print("✅ Engine SQLAlchemy siap digunakan.")
print(f"📌 Database: {target_config['database']} ({target_config['host']})\n")


# === [4] Query ambil seluruh Foreign Key ===
query_fk = """
SELECT
    table_name,
    constraint_name,
    column_name,
    referenced_table_name,
    referenced_column_name
FROM information_schema.key_column_usage
WHERE referenced_table_name IS NOT NULL
  AND table_schema = DATABASE();
"""

try:
    df_fk = pd.read_sql(query_fk, tgt_engine)
    print("📌 Foreign Key ditemukan:", len(df_fk))

    if len(df_fk) > 0:
        print("\n===== DAFTAR FOREIGN KEY =====")
        for idx, row in df_fk.iterrows():
            print(f"{idx+1}. {row['table_name']}.{row['column_name']} → "
                  f"{row['referenced_table_name']}.{row['referenced_column_name']}  "
                  f"(constraint: {row['constraint_name']})")
    else:
        print("Tidak ada foreign key ditemukan.")

except Exception as e:
    print("❌ Error saat membaca foreign key:", e)


# === [5] Waktu run ===
print("\n⏱ Terakhir Run:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
