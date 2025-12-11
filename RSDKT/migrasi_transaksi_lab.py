#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migrasi tabel: transaksi_lab (source → target)
Struktur berbeda → mapping custom.
"""

import os
import sys
import math
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime

# =====================================================
# 1. Load ENV
# =====================================================
load_dotenv()

BATCH_SIZE = int(os.getenv("MIG_BATCH_SIZE", "10000"))

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

print("==============================================================")
print("🚀 MIGRASI TABEL transaksi_lab (CUSTOM FIELD + BATCH MODE)")
print("==============================================================\n")

print(f"📌 SOURCE DB : {source_config['database']}")
print(f"📌 TARGET DB : {target_config['database']}")
print(f"📌 Batch size: {BATCH_SIZE}\n")

# =====================================================
# 2. Hitung total rows
# =====================================================
with src.connect() as conn:
    total_rows = conn.execute(
        text("SELECT COUNT(*) FROM transaksi_lab")).scalar()

print(f"📌 Total data di SOURCE : {total_rows}\n")

# =====================================================
# 3. QUERY SELECT
# =====================================================
SELECT_SQL = f"""
SELECT
    id_transaksi_lab,
    kode_transaksi_lab,
    no_order,
    no_registrasi,
    id_pasien,
    umur_tahun,
    umur_bulan,
    umur_hari,
    id_ruangan,
    id_ruangan_awal,
    perusahaan,
    id_status,
    id_petugas_lab,
    id_instalasi,
    id_kelas,
    klinik,
    nama_dokter_pengirim,
    alamat_dokter_pengirim,
    dokter_acc,
    id_dokter,
    sampel,
    jenis_sampel,
    catatan,
    status,
    jenis_permeriksaan,
    id_user,
    waktu_sampel,
    user_cekin,
    tgl_validasi,
    tgl_order,
    tgl_print,
    proses,
    prioritas,
    status_prioritas,
    diagnose,
    is_mcu,
    selesai,
    created_at,
    updated_at
FROM transaksi_lab
ORDER BY id_transaksi_lab
LIMIT {BATCH_SIZE} OFFSET :offset;
"""

# =====================================================
# 4. INSERT QUERY
# =====================================================
INSERT_SQL = """
INSERT INTO transaksi_lab (
    id_transaksi_lab,
    kode_transaksi_lab,
    no_order,
    no_registrasi,
    id_pasien,
    umur_tahun,
    umur_bulan,
    umur_hari,
    id_ruangan,
    id_asal,
    id_ruangan_awal,
    perusahaan,
    id_status,
    id_petugas_lab,
    id_instalasi,
    id_kelas,
    id_cara_masuk,
    jenis_rawat,
    klinik,
    nama_dokter_pengirim,
    alamat_dokter_pengirim,
    dokter_acc,
    id_dokter,
    sampel,
    jenis_sampel,
    catatan,
    note_analis,
    status,
    jenis_permeriksaan,
    id_user,
    waktu_sampel,
    user_cekin,
    tgl_validasi,
    tgl_order,
    tgl_print,
    proses,
    prioritas,
    status_prioritas,
    diagnose,
    klinis,
    is_mcu,
    selesai,
    created_at,
    updated_at,
    kesan,
    saran,
    newnolab
) VALUES (
    :id_transaksi_lab,
    :kode_transaksi_lab,
    :no_order,
    :no_registrasi,
    :id_pasien,
    :umur_tahun,
    :umur_bulan,
    :umur_hari,
    :id_ruangan,
    :id_asal,
    :id_ruangan_awal,
    :perusahaan,
    :id_status,
    :id_petugas_lab,
    :id_instalasi,
    :id_kelas,
    :id_cara_masuk,
    :jenis_rawat,
    :klinik,
    :nama_dokter_pengirim,
    :alamat_dokter_pengirim,
    :dokter_acc,
    :id_dokter,
    :sampel,
    :jenis_sampel,
    :catatan,
    :note_analis,
    :status,
    :jenis_permeriksaan,
    :id_user,
    :waktu_sampel,
    :user_cekin,
    :tgl_validasi,
    :tgl_order,
    :tgl_print,
    :proses,
    :prioritas,
    :status_prioritas,
    :diagnose,
    :klinis,
    :is_mcu,
    :selesai,
    :created_at,
    :updated_at,
    :kesan,
    :saran,
    :newnolab
)
"""

# =====================================================
# 5. Progress bar
# =====================================================


def progress_bar(cur, total):
    bar_len = 40
    fill = int(bar_len * cur / total)
    bar = "█" * fill + "-" * (bar_len - fill)
    pct = (cur / total * 100)
    sys.stdout.write(f"\r🔄 Progress: |{bar}| {pct:5.1f}%")
    sys.stdout.flush()


# =====================================================
# 6. MIGRASI
# =====================================================
offset = 0
inserted = 0

while True:
    with src.connect() as conn:
        rows = conn.execute(text(SELECT_SQL), {"offset": offset}).fetchall()

    if not rows:
        break

    with tgt.connect() as conn:
        tr = conn.begin()
        try:
            for row in rows:

                r = row._mapping

                id_inst = r["id_instalasi"]

                # =======================
                # MAPPING BARU
                # =======================
                id_cara_masuk = id_inst

                if id_inst == 1:
                    jenis_rawat = "RJ"
                elif id_inst == 2:
                    jenis_rawat = "RANAP"
                else:
                    jenis_rawat = "-"

                data = dict(
                    r,
                    id_asal=None,
                    id_cara_masuk=id_cara_masuk,
                    jenis_rawat=jenis_rawat,
                    note_analis=None,
                    klinis=None,
                    kesan=None,
                    saran=None,
                    newnolab=None,
                )

                conn.execute(text(INSERT_SQL), data)
                inserted += 1
                progress_bar(inserted, total_rows)

            tr.commit()

        except Exception as e:
            tr.rollback()
            print("\n❌ ERROR INSERT — BATCH ROLLBACK!")
            print(e)
            sys.exit(1)

    offset += BATCH_SIZE

print("\n\n🎉 MIGRASI transaksi_lab SELESAI!")
print(f"✔ Total dimigrasi: {inserted}")
print("⏱ Selesai pada:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("==============================================================")
