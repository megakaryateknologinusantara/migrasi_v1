#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: cek_koneksi.py
Deskripsi:
    - Koneksi ke MySQL Source & Target
    - Uji koneksi
    - Menampilkan daftar tabel pada kedua database
    - Menggunakan konfigurasi dari .env
"""

# === [1] Import library ===
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine, inspect
from datetime import datetime
from dotenv import load_dotenv
import os


# === [2] LOAD ENV FILE ===
load_dotenv()


# === [3] KONFIGURASI KONEKSI DATABASE DARI ENV ===
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


# === [4] Buat engine SQLAlchemy ===
def create_mysql_engine(cfg):
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    )


src_engine = create_mysql_engine(source_config)
tgt_engine = create_mysql_engine(target_config)

print("=======================================")
print("   🔗 CEK KONEKSI DATABASE")
print("=======================================\n")

print("⚙️  Engine SQLAlchemy siap digunakan.\n")


# === [5] Uji koneksi menggunakan mysql.connector ===
def test_connection(cfg, name):
    try:
        conn = mysql.connector.connect(**cfg)
        if conn.is_connected():
            print(
                f"✅ Koneksi berhasil ke {name}: {cfg['database']} ({cfg['host']})")
        conn.close()
    except mysql.connector.Error as e:
        print(f"❌ Gagal konek ke {name}: {e}")


test_connection(source_config, "SOURCE DB")
test_connection(target_config, "TARGET DB")


# === [6] Tampilkan daftar tabel ===
def print_table_list(engine, db_name, label):
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    print(f"\n📌 Daftar tabel di {label} ({db_name}):")
    if tables:
        for t in tables:
            print(" -", t)
    else:
        print("   (Tidak ada tabel ditemukan)")


print_table_list(src_engine, source_config['database'], "SOURCE")
print_table_list(tgt_engine, target_config['database'], "TARGET")


# === [7] Waktu run ===
print("\n⏱ Terakhir Run:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("\n=======================================\n")
