import subprocess

scripts = [
    "migrasi_kode_lab.py",
    "migrasi_kode_lab_detail.py",
    "migrasi_kode_lab_hasil.py",
    "insert_nr_single.py",
    "migrasi_antibiotik.py",
    "migrasi_bakteri.py",
    "migrasi_dokter_pj.py",
    "migrasi_dokter.py",
    "migrasi_duplo_detail.py",
    "migrasi_duplo_ori_detail.py",
    "migrasi_duplo_ori.py",
    "migrasi_duplo.py",
    "migrasi_grub_detail.py",
    "migrasi_grub.py",
    "migrasi_history.py",
    "migrasi_instalasi.py",
    "migrasi_jenis_pemeriksaan.py",
    "migrasi_kategori_alat_detail.py",
    "migrasi_kategori_alat.py",
    "migrasi_kategori_catatan.py",
    "migrasi_konten_catatan.py",
    "migrasi_kritis_detail.py",
    "migrasi_kritis.py",
    "migrasi_paket_antibiotik_detail.py",
    "migrasi_paket_antibiotik.py",
    "migrasi_paket_lab_detail.py",
    "migrasi_paket_lab.py",
    "migrasi_paket_sumsum_tulang_detail.py",
    "migrasi_paket_sumsum_tulang.py",
    "migrasi_pasien.py",
    "migrasi_petugas_lab.py",
    "migrasi_posisi_tray.py",
    "migrasi_printer_detail.py",
    "migrasi_printer.py",
    "migrasi_ruangan.py",
    "migrasi_sessions.py",
    "migrasi_setting.py",
    "migrasi_specimen.py",
    "migrasi_status_asuransi.py",
    "migrasi_status_cito.py",
    "migrasi_tat.py",
    "migrasi_transaksi_lab_detail.py",
    "migrasi_transaksi_lab.py",
    "migrasi_transaksi_paket_lab.py",
    "migrasi_users.py",
    "migrasi_waktu_pemeriksaan.py",
    "restoreFK.py"
]

for s in scripts:
    print(f"Menjalankan: {s}")
    result = subprocess.run(["python", s])

    if result.returncode != 0:
        print(f"❌ Error pada script: {s}")
        break
    else:
        print(f"✔️ Selesai: {s}")
