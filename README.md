# E-Commerce & Retail Analytics - Milestone 1
**Status output:** Data Synthesis, Data Cleansing & Storage Benchmark

**Anggota kelompok:** _(isi 5-6 nama & NIM)_

## Video Milestone 1
> **Tautan video (YouTube Unlisted / Google Drive):** _(tempel tautan di sini)_  - durasi 3-5 menit

## Struktur repositori
```
data_generator/synthetic_generator.py              # pembuat data sintetis + injeksi data kotor
notebooks/01_cleansing_and_storage_benchmark.ipynb # profiling, cleansing, validasi, benchmark (output tersimpan)
data/benchmark/                                    # hasil benchmark (csv) & grafik (png)
requirements.txt
```
Data besar (`data/raw`, `data/clean`) tidak di-commit; buat ulang dengan langkah berikut.

## Cara menjalankan
```bash
pip install -r requirements.txt
cd data_generator && python synthetic_generator.py --out-dir ../data/raw   # +-15 detik, seed=42
cd ../notebooks && jupyter nbconvert --to notebook --execute --inplace 01_cleansing_and_storage_benchmark.ipynb
```
Parameter generator: `--n-transactions`, `--n-customers`, `--n-events`, `--dirty-rate` (0.05-0.10), `--seed`.

## Big Data Planning Canvas (ringkasan)
| Elemen | Isi |
|---|---|
| **Masalah bisnis** | Platform e-commerce perlu memahami perilaku pelanggan (siapa yang bernilai tinggi, kapan churn, kanal/kota mana yang tumbuh) dari data transaksi, profil, dan clickstream yang besar dan tidak bersih. |
| **Target analisis** | Cohort analysis (retensi per bulan daftar), segmentasi RFM, funnel clickstream, dan benchmark performa CSV vs Parquet pada agregasi besar. |
| **Volume** | `transactions` 1.524.000 baris (raw) -> 1.458.000 (clean); `customers` 75.600 -> 75.000; `clickstream` 303.600 -> 294.720 event. |
| **Variety** | Relasional (CSV: transaksi & pelanggan) + semi-terstruktur (JSON Lines: clickstream). Kunci gabungan: `customer_id` = `user_id`. |
| **Velocity** | Batch pada Milestone 1; clickstream diperlakukan sebagai log event bertimestamp (dasar sessionization & near-real-time di Milestone 2). |
| **Veracity** | Anomali sengaja disuntikkan (+-7,9% baris per dataset, target 5-10%): null, format kota/gender/device tidak konsisten, price/quantity tidak valid, tanggal masa depan, duplikat ID. |
| **Value** | Targeting promosi per segmen RFM, peningkatan retensi, optimasi biaya penyimpanan & kecepatan query. |
| **Teknologi** | Python (pandas, pyarrow) untuk M1; PySpark + Parquet (arsitektur bronze/silver/gold) untuk M2. |
| **Risiko** | Imputasi kategori dapat bias; baris tanpa `customer_id` dibuang (kehilangan transaksi tamu); benchmark bergantung pada spesifikasi mesin. |

### Skema 3 dataset
| Dataset | Kolom |
|---|---|
| `transactions.csv` (Fact) | transaction_id, customer_id, timestamp, product_id, category, quantity, price, discount, payment_method, city |
| `customers.csv` (Dim) | customer_id, age_group, gender, signup_date, membership_tier, city |
| `clickstream.json` (JSONL) | event_id, session_id, user_id, timestamp, page_url, action_event, device_type |

## Injeksi data kotor & cleansing
Setiap baris kotor hanya mendapat satu jenis anomali (alokasi disjoint), sehingga persentase dapat diverifikasi. Hasil deteksi di notebook identik dengan `injection_log.json`.

| Dataset | Baris kotor | Persentase | Baris dibuang saat cleansing |
|---|---|---|---|
| transactions | 120.000 | 7,87% | 66.000 (4,33%) |
| customers | 6.000 | 7,94% | 600 (0,79%) |
| clickstream | 24.000 | 7,91% | 8.880 (2,92%) |

Aturan lengkap ada di bagian 3 notebook. Ringkas: duplikat dihapus; `customer_id`/`timestamp` null dan tanggal masa depan dibuang; `category` null diimputasi dari `product_id` (10.800 baris pulih, 0 `Unknown`); 51 variasi penulisan kota dinormalisasi menjadi 10 kota; `price <= 0` dan `quantity <= 0` dibuang. Validasi pasca-cleansing: semua `assert` lolos, integritas referensial 100%.

## Hasil benchmark (median 3x, mesin 2 vCPU / 7 GB)
Ukuran file:

| Format | transactions | clickstream |
|---|---|---|
| CSV | 130,6 MB | 22,0 MB |
| JSONL | 330,3 MB | 52,4 MB |
| Parquet snappy | 37,1 MB (-72% vs CSV) | 6,9 MB |
| Parquet zstd | 26,9 MB (-79% vs CSV) | 4,8 MB |

Waktu (detik) pada `transactions`:

| Workload | CSV | JSONL | Parquet snappy | Parquet zstd |
|---|---|---|---|---|
| Baca penuh | 2,96 | 9,87 | 0,20 | 0,23 |
| Q1 revenue kota x kategori | 1,19 | 10,12 | 0,18 | 0,19 |
| Q2 frekuensi & monetary per customer | 1,52 | 10,54 | 0,20 | 0,32 |
| Q3 revenue bulanan | 1,62 | 10,79 | 0,17 | 0,16 |

Parquet 6-15x lebih cepat dari CSV dan sekitar 50x lebih cepat dari JSONL pada data transaksi. Tabel clickstream dan grafik lengkap ada di notebook dan `data/benchmark/`.

**Rencana M2 (PySpark):** bronze (skema eksplisit, partisi per bulan) -> silver (cleansing, broadcast join dimensi customers) -> gold (RFM, cohort, funnel/sessionization dengan window function); uji AQE, jumlah shuffle partition, dan caching; bandingkan pandas vs PySpark.

## Naskah video (3-5 menit)
| Bagian | Durasi | Pembicara | Isi |
|---|---|---|---|
| 1 | 1,0 mnt | 1 | Latar belakang masalah, target analisis, skema 3 dataset (tabel di atas). |
| 2 | 1,5 mnt | 2 | Jalankan generator, tunjukkan tabel profiling kekotoran (7,9%) dan `injection_log.json`; demo fungsi cleansing, log langkah, dan validasi `assert`. |
| 3 | 1,5 mnt | 1-2 | Eksekusi benchmark: ukuran file dan waktu baca/query CSV vs JSONL vs Parquet (tampilkan grafik). |
| 4 | 1,0 mnt | 1 | Ringkasan hasil (Parquet -72% ukuran, 6-15x lebih cepat) dan rencana arsitektur PySpark M2. |
