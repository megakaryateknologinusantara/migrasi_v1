-- Backup FK dari database `db_slims_rsdkt_aio`
-- Generated: 2025-12-11T13:26:48.636344

-- RESTORE STATEMENTS:

ALTER TABLE `analis`
  ADD CONSTRAINT `analis_id_user_foreign`
  FOREIGN KEY (`id_user`)
  REFERENCES `users` (`id`);

ALTER TABLE `barang_keluar`
  ADD CONSTRAINT `barang_keluar_barang_id_foreign`
  FOREIGN KEY (`barang_id`)
  REFERENCES `barangs` (`id`);

ALTER TABLE `barang_keluar`
  ADD CONSTRAINT `barang_keluar_id_transaksi_stok_foreign`
  FOREIGN KEY (`id_transaksi_stok`)
  REFERENCES `transaksi_stok` (`id`);

ALTER TABLE `barang_keluar_detail`
  ADD CONSTRAINT `barang_keluar_detail_barang_keluar_id_foreign`
  FOREIGN KEY (`barang_keluar_id`)
  REFERENCES `barang_keluar` (`id`);

ALTER TABLE `barang_keluar_detail`
  ADD CONSTRAINT `barang_keluar_detail_barang_masuk_id_foreign`
  FOREIGN KEY (`barang_masuk_id`)
  REFERENCES `barang_masuk` (`id`);

ALTER TABLE `barang_masuk`
  ADD CONSTRAINT `barang_masuk_barang_id_foreign`
  FOREIGN KEY (`barang_id`)
  REFERENCES `barangs` (`id`);

ALTER TABLE `barang_masuk`
  ADD CONSTRAINT `barang_masuk_id_transaksi_stok_foreign`
  FOREIGN KEY (`id_transaksi_stok`)
  REFERENCES `transaksi_stok` (`id`);

ALTER TABLE `kode_lab`
  ADD CONSTRAINT `kode_lab_id_spesimen_snomed_foreign`
  FOREIGN KEY (`id_spesimen_snomed`)
  REFERENCES `spesimen_snomed` (`id`);

ALTER TABLE `media_pemeriksaan`
  ADD CONSTRAINT `media_pemeriksaan_barang_id_foreign`
  FOREIGN KEY (`barang_id`)
  REFERENCES `barangs` (`id`);

ALTER TABLE `media_pemeriksaan`
  ADD CONSTRAINT `media_pemeriksaan_id_kode_lab_foreign`
  FOREIGN KEY (`id_kode_lab`)
  REFERENCES `kode_lab` (`id_kode_lab`);

ALTER TABLE `qc_history`
  ADD CONSTRAINT `qc_history_id_qc_transaksi_foreign`
  FOREIGN KEY (`id_qc_transaksi`)
  REFERENCES `qc_transaksi` (`id`);

ALTER TABLE `qc_history`
  ADD CONSTRAINT `qc_history_id_user_foreign`
  FOREIGN KEY (`id_user`)
  REFERENCES `users` (`id`);

ALTER TABLE `qc_transaksi`
  ADD CONSTRAINT `qc_transaksi_id_qc_setup_foreign`
  FOREIGN KEY (`id_qc_setup`)
  REFERENCES `qc_setup` (`id`);

ALTER TABLE `qc_transaksi`
  ADD CONSTRAINT `qc_transaksi_id_user_foreign`
  FOREIGN KEY (`id_user`)
  REFERENCES `users` (`id`);

ALTER TABLE `qc_transaksi_detail`
  ADD CONSTRAINT `qc_transaksi_detail_id_kode_lab_foreign`
  FOREIGN KEY (`id_kode_lab`)
  REFERENCES `kode_lab` (`id_kode_lab`);

ALTER TABLE `qc_transaksi_detail`
  ADD CONSTRAINT `qc_transaksi_detail_id_qc_transaksi_foreign`
  FOREIGN KEY (`id_qc_transaksi`)
  REFERENCES `qc_transaksi` (`id`);

ALTER TABLE `transaksi_lab`
  ADD CONSTRAINT `transaksi_lab_id_asal_foreign`
  FOREIGN KEY (`id_asal`)
  REFERENCES `asal_rujukan` (`id`);

ALTER TABLE `transaksi_lab_detail`
  ADD CONSTRAINT `transaksi_lab_detail_ibfk_2`
  FOREIGN KEY (`id_transaksi_lab`)
  REFERENCES `transaksi_lab` (`id_transaksi_lab`);

ALTER TABLE `transaksi_lab_detail`
  ADD CONSTRAINT `transaksi_lab_detail_id_asal_foreign`
  FOREIGN KEY (`id_asal`)
  REFERENCES `asal_rujukan` (`id`);

ALTER TABLE `transaksi_lab_mb`
  ADD CONSTRAINT `transaksi_lab_mb_id_pasien_foreign`
  FOREIGN KEY (`id_pasien`)
  REFERENCES `pasien` (`id_pasien`);

ALTER TABLE `transaksi_lab_mb`
  ADD CONSTRAINT `transaksi_lab_mb_id_ruangan_foreign`
  FOREIGN KEY (`id_ruangan`)
  REFERENCES `ruangan` (`id_ruangan`);

ALTER TABLE `transaksi_lab_mb`
  ADD CONSTRAINT `transaksi_lab_mb_id_specimen_foreign`
  FOREIGN KEY (`id_specimen`)
  REFERENCES `specimen` (`id`);

ALTER TABLE `transaksi_lab_mb_detail`
  ADD CONSTRAINT `transaksi_lab_mb_detail_id_asal_foreign`
  FOREIGN KEY (`id_asal`)
  REFERENCES `asal_rujukan` (`id`);

ALTER TABLE `transaksi_lab_mb_detail`
  ADD CONSTRAINT `transaksi_lab_mb_detail_id_kode_lab_foreign`
  FOREIGN KEY (`id_kode_lab`)
  REFERENCES `kode_lab` (`id_kode_lab`);

ALTER TABLE `transaksi_lab_mb_detail`
  ADD CONSTRAINT `transaksi_lab_mb_detail_id_transaksi_lab_mb_foreign`
  FOREIGN KEY (`id_transaksi_lab_mb`)
  REFERENCES `transaksi_lab_mb` (`id`);

ALTER TABLE `transaksi_lab_mb_mikroorganisme`
  ADD CONSTRAINT `transaksi_lab_mb_mikroorganisme_id_mikroorganisme_foreign`
  FOREIGN KEY (`id_mikroorganisme`)
  REFERENCES `mikroorganisme` (`id`);

ALTER TABLE `transaksi_lab_mb_mikroorganisme`
  ADD CONSTRAINT `transaksi_lab_mb_mikroorganisme_id_transaksi_lab_mb_foreign`
  FOREIGN KEY (`id_transaksi_lab_mb`)
  REFERENCES `transaksi_lab_mb` (`id`);

