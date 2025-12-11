#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: restore_foreign_key.py
Deskripsi:
    - Membaca file SQL backup FK
    - Eksekusi ADD CONSTRAINT di dalam transaction
    - Jika error -> rollback
"""

import os
from sqlalchemy import create_engine
from datetime import datetime


# === [1] KONFIGURASI FILE BACKUP (HARD-CODED) ===

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Nama file backup (boleh kamu ganti sesuai file)
backup_filename = "backup_fk_db_slims_rsdkt_aio_20251203_095015.sql"

# Path lengkap → otomatis mengarah ke folder script
sql_backup_file = os.path.join(BASE_DIR, backup_filename)
# Ganti sesuai nama file backup kamu


# === [2] Konfigurasi DATABASE TARGET ===
target_config = {
    'host': 'localhost',
    'user': 'dorisjuarsa',
    'password': 'dorisjuarsa',
    'database': 'db_slims_rsdkt_aio'
}


# === [3] Buat ENGINE SQLAlchemy ===
def create_mysql_engine(cfg):
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    )


tgt_engine = create_mysql_engine(target_config)
print("✅ Engine SQLAlchemy siap digunakan.\n")


# === [4] Fungsi BACA FILE SQL ===
def read_sql_file(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"File tidak ditemukan: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# === [5] Pecah SQL menjadi statements ===
def split_sql_statements(sql_text):
    statements = []
    buffer = ""

    for line in sql_text.splitlines():
        clean = line.strip()
        if not clean or clean.startswith("--") or clean.startswith("/*"):
            continue

        buffer += line + "\n"
        if clean.endswith(";"):
            statements.append(buffer.strip())
            buffer = ""

    if buffer.strip():
        statements.append(buffer.strip())

    return statements


# === [6] Fungsi RESTORE FK ===
def restore_fk(sql_path):
    print(f"📄 Membaca file: {sql_path}\n")

    sql_text = read_sql_file(sql_path)
    statements = split_sql_statements(sql_text)

    if not statements:
        print("❌ Tidak ada perintah SQL valid di file.")
        return

    print(f"🔍 Total statement yang akan dijalankan: {len(statements)}\n")
    success = 0
    fail = 0
    errors = []

    print("🔄 Menjalankan RESTORE dengan transaction...\n")

    with tgt_engine.begin() as conn:
        for stmt in statements:
            try:
                conn.exec_driver_sql(stmt)
                success += 1
            except Exception as e:
                fail += 1
                errors.append((stmt, str(e)))
                print("⚠️ Error → rollback transaksi...\n")
                raise

    print("===================================")
    print("✅ RESTORE SELESAI")
    print(f"   ✔ Berhasil : {success}")
    print(f"   ❌ Gagal    : {fail}")
    print("===================================")

    if errors:
        print("\n📌 Error detail (contoh 5):")
        for stmt, msg in errors[:5]:
            print("----- SQL -----")
            print(stmt)
            print("----- ERROR -----")
            print(msg)
            print()


# === [7] MAIN EXECUTION ===
if __name__ == "__main__":
    print("🚀 RESTORE FOREIGN KEY — MODE HARDCODE FILE\n")

    try:
        restore_fk(sql_backup_file)
    except Exception as e:
        print("\n❌ Restore gagal:", e)

    print("\n⏱ Selesai:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
