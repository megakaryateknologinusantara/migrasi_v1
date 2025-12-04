#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi tabel: users (source → target)
Struktur berbeda:
    - TARGET punya kolom 'permissions' yang tidak ada di SOURCE
    - Setiap row harus diberi default permissions:
      ["master_lab","qc","master_periksa","workspace","stok","report"]

Support:
    - Batch processing (MIG_BATCH_SIZE dari .env)
    - Progress bar
"""

from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
import sys
import json


# === [1] Load ENV ===
load_dotenv()
BATCH_SIZE = int(os.getenv("MIG_BATCH_SIZE", "10000"))


# === [2] Database config ===
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


# === [3] Engine maker ===
def make_engine(cfg):
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    )


src = make_engine(source_config)
tgt = make_engine(target_config)


print("=================================================")
print("🚀 MIGRASI TABEL users (BATCH + PERMISSIONS)")
print("=================================================\n")

print(f"📌 SOURCE DB : {source_config['database']}")
print(f"📌 TARGET DB : {target_config['database']}")
print(f"📌 Batch size: {BATCH_SIZE}\n")


# === [4] Hitung total row ===
with src.connect() as conn:
    total_rows = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()

print(f"📌 Total data di SOURCE: {total_rows}\n")


# === [5] SELECT batch dari SOURCE ===
SELECT_BATCH = f"""
SELECT
    id,
    name,
    email,
    sip,
    email_verified_at,
    password,
    foto,
    ttd,
    level,
    two_factor_secret,
    two_factor_recovery_codes,
    remember_token,
    current_team_id,
    profile_photo_path,
    created_at,
    updated_at
FROM users
ORDER BY id
LIMIT {BATCH_SIZE} OFFSET :offset;
"""


# === [6] INSERT ke TARGET ===
INSERT_SQL = """
INSERT INTO users (
    id,
    name,
    email,
    sip,
    email_verified_at,
    password,
    foto,
    ttd,
    level,
    permissions,
    two_factor_secret,
    two_factor_recovery_codes,
    remember_token,
    current_team_id,
    profile_photo_path,
    created_at,
    updated_at
) VALUES (
    :id,
    :name,
    :email,
    :sip,
    :email_verified_at,
    :password,
    :foto,
    :ttd,
    :level,
    :permissions,
    :two_factor_secret,
    :two_factor_recovery_codes,
    :remember_token,
    :current_team_id,
    :profile_photo_path,
    :created_at,
    :updated_at
)
"""


# === [7] DEFAULT PERMISSIONS ===
DEFAULT_PERM = json.dumps([
    "master_lab", "qc", "master_periksa",
    "workspace", "stok", "report"
])


# === [8] Progress bar ===
def progress_bar(current, total):
    bar_len = 40
    filled = int(bar_len * current / total) if total > 0 else 40
    bar = "█" * filled + "-" * (bar_len - filled)
    percent = (current / total * 100) if total > 0 else 100
    sys.stdout.write(f"\r🔄 Progress: |{bar}| {percent:5.1f}%")
    sys.stdout.flush()


# === [9] MIGRASI per batch ===
offset = 0
total_inserted = 0

while True:

    # Ambil batch dari source
    with src.connect() as conn:
        rows = conn.execute(text(SELECT_BATCH), {"offset": offset}).fetchall()

    if not rows:
        break

    # Insert batch ke target
    with tgt.connect() as conn:
        trans = conn.begin()
        try:
            for row in rows:
                data = dict(row._mapping)

                # inject default permissions
                data["permissions"] = DEFAULT_PERM

                conn.execute(text(INSERT_SQL), data)

            trans.commit()
        except Exception as e:
            trans.rollback()
            print("\n❌ ERROR insert batch, rollback dilakukan!")
            print("Error:", e)
            exit(1)

    total_inserted += len(rows)
    offset += BATCH_SIZE

    progress_bar(total_inserted, total_rows)

print()


# === [10] Selesai ===
print("\n🎉 MIGRASI users SELESAI!")
print(f"✔ Total baris dimigrasikan: {total_inserted}")
print("⏱ Waktu selesai =", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("=================================================\n")
