#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: drop_foreign_key.py
Deskripsi:
    - Mengambil semua foreign key dari database target
    - Membuat file backup SQL untuk restore FK
    - Drop semua FK secara atomik (transaction)
    - Menggunakan konfigurasi dari .env
"""

import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


# === [1] LOAD ENV FILE ===
load_dotenv()


# === [2] KONFIG DATABASE TARGET via ENV ===
target_config = {
    'host': os.getenv('TGT_HOST'),
    'user': os.getenv('TGT_USER'),
    'password': os.getenv('TGT_PASSWORD'),
    'database': os.getenv('TGT_DATABASE'),
}


# === [3] Buat ENGINE SQLAlchemy ===
def create_mysql_engine(cfg):
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    )


tgt_engine = create_mysql_engine(target_config)
print("=======================================")
print("🔗 DROP FOREIGN KEYS -- USING .env")
print("=======================================\n")

print(f"📌 Target DB   : {target_config['database']}")
print(f"📌 Target Host : {target_config['host']}\n")


# === [4] Ambil semua foreign key ===
query_fk = """
SELECT
    table_name,
    constraint_name,
    column_name,
    referenced_table_name,
    referenced_column_name,
    ordinal_position
FROM information_schema.key_column_usage
WHERE referenced_table_name IS NOT NULL
  AND table_schema = DATABASE()
ORDER BY table_name, constraint_name, ordinal_position;
"""

df_fk = pd.read_sql(query_fk, tgt_engine)

if df_fk.empty:
    print("📌 Tidak ditemukan foreign key. Tidak ada yang dihapus.")
    exit(0)


# === [5] Group FK multi-column ===
grouped = (
    df_fk
    .groupby(['table_name', 'constraint_name', 'referenced_table_name'], sort=False)
    .agg({
        'column_name': lambda s: list(s),
        'referenced_column_name': lambda s: list(s)
    })
    .reset_index()
)

restore_statements = []
drop_statements = []


# === [6] Generate SQL restore + drop ===
for _, row in grouped.iterrows():
    tname = row['table_name']
    cname = row['constraint_name']
    ref_table = row['referenced_table_name']
    cols = row['column_name']
    refcols = row['referenced_column_name']

    cols_sql = ", ".join([f"`{c}`" for c in cols])
    refcols_sql = ", ".join([f"`{c}`" for c in refcols])

    restore_sql = (
        f"ALTER TABLE `{tname}`\n"
        f"  ADD CONSTRAINT `{cname}`\n"
        f"  FOREIGN KEY ({cols_sql})\n"
        f"  REFERENCES `{ref_table}` ({refcols_sql});"
    )

    drop_sql = f"ALTER TABLE `{tname}` DROP FOREIGN KEY `{cname}`;"

    restore_statements.append(restore_sql)
    drop_statements.append(drop_sql)


# === [7] Simpan backup SQL ===
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"backup_fk_{tgt_engine.url.database}_{ts}.sql"

with open(filename, "w", encoding="utf-8") as f:
    f.write(f"-- Backup FK dari database `{tgt_engine.url.database}`\n")
    f.write(f"-- Generated: {datetime.now().isoformat()}\n\n")
    f.write("-- RESTORE STATEMENTS:\n\n")
    for stmt in restore_statements:
        f.write(stmt + "\n\n")

print(f"✅ File backup tersimpan: {filename}")


# === [8] Drop FK dalam transaction ===
print("\n🔄 Menjalankan DROP FOREIGN KEY ...")

with tgt_engine.connect() as conn:
    trans = conn.begin()
    try:
        for sql in drop_statements:
            conn.exec_driver_sql(sql)
        trans.commit()
        print(f"✅ Berhasil menghapus {len(drop_statements)} foreign key.")
    except Exception as e:
        trans.rollback()
        print("❌ Gagal menghapus FK (rollback dilakukan).")
        print("Error:", e)
        exit(1)


# === [9] Ringkasan ===
print("\n📌 Ringkasan:")
print(f"- Total FK ditemukan : {len(grouped)} constraint(s)")
print(f"- File backup        : {filename}")
print(f"- Contoh DROP FK     :")

for s in drop_statements[:10]:
    print("  ", s)

print("\n⏱ Selesai:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("\n=======================================\n")
