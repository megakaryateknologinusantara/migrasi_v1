#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: truncate_except_laravel.py
Deskripsi:
    - Mengosongkan SEMUA tabel pada database target
    - KECUALI tabel penting Laravel + tambahan user
    - Menggunakan konfigurasi dari .env
"""

from sqlalchemy import create_engine, inspect
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

print("=======================================")
print("      🔥 TRUNCATE DATABASE SAFELY")
print("=======================================\n")
print(f"📌 Target DB   : {target_config['database']}")
print(f"📌 Target Host : {target_config['host']}\n")


# === [4] Tabel yang tidak boleh dihapus ===
protected_tables = {
    # "users",
    "migrations",
    "password_resets",
    "personal_access_tokens",
    "failed_jobs",
    "jobs",
    "sessions",
    "cara_masuk",
    "config",
    "asal_rujukan",
}


# === [5] Ambil semua tabel ===
inspector = inspect(tgt_engine)
all_tables = inspector.get_table_names()

print(f"📌 Total tabel ditemukan : {len(all_tables)}\n")


# === [6] Tentukan tabel yang boleh di-truncate ===
truncate_tables = [t for t in all_tables if t not in protected_tables]

print("🔍 TABEL YANG AKAN DI-TRUNCATE:")
for t in truncate_tables:
    print(" -", t)

print("\n🔒 TABEL YANG DILINDUNGI (SKIP):")
for t in protected_tables:
    print(" -", t)


# === [7] Eksekusi truncate dengan FK_CHECKS OFF ===
with tgt_engine.connect() as conn:
    trans = conn.begin()
    try:
        print("\n🚫 Menonaktifkan FOREIGN_KEY_CHECKS...")
        conn.exec_driver_sql("SET FOREIGN_KEY_CHECKS = 0;")

        for table in truncate_tables:
            sql = f"TRUNCATE `{table}`;"
            conn.exec_driver_sql(sql)
            print(f"   ✔ TRUNCATED: {table}")

        print("\n🔁 Mengaktifkan kembali FOREIGN_KEY_CHECKS...")
        conn.exec_driver_sql("SET FOREIGN_KEY_CHECKS = 1;")

        trans.commit()
        print("\n🎉 BERHASIL! Semua tabel yang tidak dilindungi telah di-empty.")
    except Exception as e:
        trans.rollback()
        print("❌ ERROR! Semua perubahan dibatalkan.")
        print("   ➜", e)


print("\n⏱ Selesai:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("\n=======================================\n")
