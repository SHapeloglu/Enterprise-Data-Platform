# ApacheFabric — Open Data Fabric POC

AWS üzerinde Apache tabanlı bir açık veri platformu (Open Data Fabric) kavram kanıtı (POC). Amaç, açık kaynak bileşenlerle uçtan uca bir lakehouse mimarisini (ingestion → storage → catalog → SQL sorgulama → BI görselleştirme) kurup doğrulamak.

## Mimari

```
NiFi (ingestion) → S3 (bronze) → Spark (transform) → Iceberg tablosu (Glue Catalog) → Trino (SQL) → Superset (BI)
                                                                                    ↑
                                                                              Airflow (orkestrasyon)
```

Orijinal tasarımda yer alan bazı bileşenler, POC'yi hızlandırmak için AWS'in yönetilen servisleriyle değiştirildi:

| Orijinal tasarım | POC'de kullanılan | Neden |
|---|---|---|
| Apache Ozone | AWS S3 | Zaten AWS üzerindeyiz, ek container'a gerek yok |
| Gravitino / Hive Metastore | AWS Glue Data Catalog | Yönetilen servis, ek container'a gerek yok |

## Altyapı

- **EC2**: `ApacheFabric`, eu-central-1 (Frankfurt), `c7i.2xlarge` (8 vCPU, 16 GiB), Ubuntu Server 24.04 LTS
- **Elastic IP**: sabit public IP, restart'larda değişmez
- **S3**: `nexmeet-odf-lakehouse` — lakehouse'un fiziksel depolama katmanı
- **IAM Role**: `odf-ec2-role`, S3 (list/get/put/delete, bucket'a scoped) ve Glue (database/table/partition) izinleriyle
- **Güvenlik grubu**: SSH (22), Trino (8080), Superset (8088), NiFi (8443), Airflow (8090) — hepsi sadece belirli IP'ye açık

## Servisler (Docker Compose — `~/odf-deploy`)

| Servis | Image | Port | Not |
|---|---|---|---|
| `odf-trino` | `trinodb/trino:latest` | 8080 | Iceberg catalog, `catalog.type=glue` |
| `odf-spark` | `apache/spark:3.5.3-python3` | — | Idle container, job'lar `docker exec` ile tetiklenir |
| `odf-superset` | custom (`Dockerfile.superset`) | 8088 | Trino SQLAlchemy sürücüsü eklenmiş |
| `odf-nifi` | `apache/nifi:latest` | 8443 | Ingestion flow'ları |
| `odf-airflow` | custom (`Dockerfile.airflow`) | 8090 | Standalone mode, orkestrasyon |

Tüm servisleri ayağa kaldırmak için:
```bash
cd ~/odf-deploy
docker compose up -d --build
```

## Veri akışı (doğrulandı)

1. **NiFi**: `InvokeHTTP → PutS3Object` flow'u, public bir CSV kaynağını çekip `s3://nexmeet-odf-lakehouse/bronze/iris/sales.csv` altına indirir (IAM role üzerinden, `AWSCredentialsProviderControllerService` ile).
2. **Spark**: Bronze CSV'yi okuyup `glue_catalog.odf_bronze.sales` adında bir Iceberg tablosuna yazar (32.718 satır).
3. **Trino**: Tablo `iceberg.odf_bronze.sales` olarak SQL ile sorgulanabilir.
4. **Superset**: Trino bağlantısı üzerinden tabloya karşı dashboard/chart oluşturulur.

NiFi flow'u `NiFi_Flow.json` olarak export edilmiş ve `~/nifi-templates/` altında yedeklenmiştir.

## Erişim bilgileri

- **NiFi**: `https://odf-nifi.local:8443` (hosts dosyasında Elastic IP'ye map'li — SNI doğrulaması nedeniyle ham IP ile çalışmaz) — `admin` / *(bkz. güvenli not defteri)*
- **Superset**: `http://<elastic-ip>:8088` — `admin` / *(bkz. güvenli not defteri)*
- **Trino**: `http://<elastic-ip>:8080`
- **Airflow**: `http://<elastic-ip>:8090`

> **Not:** Bu bir POC ortamıdır — SQLite metadata DB, dev server modunda çalışan servisler ve POC'ye özel kimlik bilgileri içerir. Production'a taşımadan önce sertleştirme (hardening) gereklidir.

## Yol haritası

- [x] **Phase 1 — Core**: Storage → Catalog → SQL → BI zinciri (S3 + Glue + Trino + Superset)
- [x] **Phase 1.5 — Gerçek kaynaktan ingestion**: NiFi ile dış kaynaktan veri çekme
- [ ] **Phase 2 — Orkestrasyon**: Airflow DAG'ı ile NiFi flow'unu ve Spark job'ını zamanlanmış şekilde tetikleme
- [ ] **Phase 3 — Governance**: OpenLineage / Kylin POC
- [ ] **Phase 4 — Gateway & Observability**

## Kurulum sırasında çözülen notlar

- `bitnami/spark:3.5` ve `trinodb/trino:465` Docker Hub'da artık çözümlenmiyor → `apache/spark:3.5.3-python3` ve `trinodb/trino:latest` kullanıldı.
- Superset'in varsayılan image'ında pip install `/app/.venv`'e yazamıyordu (non-root kullanıcı) → `Dockerfile.superset` ile root olarak sürücü kurulup sonra `superset` kullanıcısına geri dönüldü.
- NiFi 2.x'te "Create Template" özelliği yok (1.x'ten farklı) → flow export'u için "Download Flow Definition → With External Services" kullanıldı.
