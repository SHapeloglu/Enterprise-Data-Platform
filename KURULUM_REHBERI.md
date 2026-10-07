# ApacheFabric POC — Adım Adım Kurulum Rehberi

Bu doküman, projenin şu ana kadar yapılan tüm kurulum adımlarını ve kullanılan komutları sırasıyla içerir.

---

## 1. AWS Altyapısı (önceden kurulmuş)

- EC2 instance: `ApacheFabric`, eu-central-1 (Frankfurt), `c7i.2xlarge`, Ubuntu Server 24.04 LTS
- Elastic IP: `63.181.138.9` (sabit, restart'larda değişmez)
- S3 bucket: `nexmeet-odf-lakehouse`
- IAM Role: `odf-ec2-role` (S3 + Glue izinleriyle, instance'a attached)
- Güvenlik grubu (`launch-wizard-3`): SSH (22), Trino (8080), Superset (8088), NiFi (8443), Airflow (8090) — sadece kendi IP'ne açık
- Key pair: `nexmeet.pem`, local'de `C:\Users\yeliz\Desktop\Projeler\GitHub\VideoConference\nexmeet.pem`

Docker Compose stack'i `~/odf-deploy` altında, şu servislerle ayakta: `odf-trino`, `odf-spark`, `odf-superset`, `odf-nifi`.

---

## 2. NiFi Flow'u Kurma (InvokeHTTP → PutS3Object)

### 2.1 NiFi'a giriş
`https://odf-nifi.local:8443` adresine git (ham IP ile SNI hatası alırsın; Windows hosts dosyasına `63.181.138.9 odf-nifi.local` eklenmiş olmalı). Giriş: `admin` / `odfNifiPass123!`

### 2.2 InvokeHTTP processor'ü ekle ve yapılandır
- Canvasa `InvokeHTTP` processor'ü sürükle.
- **Properties** sekmesinde:
  - `HTTP Method` = `GET`
  - `HTTP URL` = `https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/sales.csv`
- **Relationships** sekmesinde `Failure`, `No Retry`, `Original` için **terminate** işaretle (Response hariç, o PutS3Object'e bağlanacak).

### 2.3 PutS3Object processor'ü ekle ve yapılandır
- Canvasa `PutS3Object` processor'ü ekle.
- InvokeHTTP'den PutS3Object'e bağlantı çiz, relationship olarak sadece **Response**'u işaretle.
- **Properties** sekmesinde:
  - `Bucket` = `nexmeet-odf-lakehouse`
  - `Object Key` = `bronze/iris/sales.csv`
  - `Region` = `Europe (Frankfurt)` / `eu-central-1`
  - `AWS Credentials Provider Service` → **Create new service** → `AWSCredentialsProviderControllerService`
- **Relationships** sekmesinde `success` ve `failure` için **terminate** işaretle.

### 2.4 Credentials service'i etkinleştir
- Controller service'in **Properties** sekmesinde `Use Default Credentials` = `true` (diğer alanlar boş — EC2 IAM role'ü kullanılır).
- Servisi **Enable** et (⚡ ikonu).

### 2.5 Flow'u çalıştır ve doğrula
```bash
# EC2 terminalinde:
aws s3 ls s3://nexmeet-odf-lakehouse/bronze/iris/ --human-readable
aws s3 cp s3://nexmeet-odf-lakehouse/bronze/iris/sales.csv - | head -5
```
> ⚠️ Dikkat: InvokeHTTP'nin `Run Schedule`'ını `0 sec` bırakırsan processor sürekli/aralıksız tetiklenir. Test sonrası `60 sec` gibi makul bir değere çek, ve biriken kuyruğu (Response bağlantısı → sağ tık → **Empty Queue**) temizle.

### 2.6 Flow'u template olarak export et (yedek)
NiFi 2.x'te "Create Template" yok — bunun yerine:
- Canvasta boş alana sağ tık → **Download Flow Definition → With External Services**
- İnen `.json` dosyasını EC2'ye de yedekle:
```bash
# EC2'de klasör oluştur
mkdir -p ~/nifi-templates

# Windows'ta (pem dosyasının olduğu dizinden):
scp -i nexmeet.pem C:\Users\yeliz\Downloads\NiFi_Flow.json ubuntu@63.181.138.9:~/nifi-templates/
```

---

## 3. AWS CLI Kurulumu (EC2 üzerinde)

```bash
sudo apt update && sudo apt install -y awscli
```

---

## 4. Glue Database Oluşturma

Spark job'ının yazacağı Iceberg tablosu için önce Glue'da database oluşturulmalı:

```bash
aws glue create-database --database-input '{"Name": "odf_bronze"}' --region eu-central-1
```

---

## 5. Spark Job'ı — Bronze CSV'yi Iceberg Tablosuna Yazma

### 5.1 Script'i yükle
```bash
# Windows'ta:
cd C:\Users\yeliz\Desktop\Projeler\GitHub\VideoConference
scp -i nexmeet.pem C:\Users\yeliz\Downloads\bronze_to_iceberg_sales.py ubuntu@63.181.138.9:~/

# EC2'de container'a kopyala:
docker cp ~/bronze_to_iceberg_sales.py odf-spark:/tmp/bronze_to_iceberg_sales.py
```

### 5.2 Script içeriği (`bronze_to_iceberg_sales.py`)
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("bronze_sales_to_iceberg").getOrCreate()

# 1) Read the raw CSV landed by NiFi
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("s3a://nexmeet-odf-lakehouse/bronze/iris/sales.csv")

print(f"Row count read from bronze CSV: {df.count()}")
df.printSchema()

# 2) Write it out as a managed Iceberg table via the Glue catalog
df.writeTo("glue_catalog.odf_bronze.sales").createOrReplace()

print("Wrote glue_catalog.odf_bronze.sales")

# 3) Sanity check: read it back through the catalog
spark.table("glue_catalog.odf_bronze.sales").show(5, truncate=False)

spark.stop()
```

### 5.3 Job'ı çalıştır
```bash
docker exec -it odf-spark /opt/spark/bin/spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
  --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.glue_catalog.warehouse=s3://nexmeet-odf-lakehouse/warehouse \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  /tmp/bronze_to_iceberg_sales.py
```

**Sonuç:** 32.718 satır okundu, `glue_catalog.odf_bronze.sales` tablosuna yazıldı, geri okunarak doğrulandı.

> Not: `spark-submit` PATH'te değil, tam yol (`/opt/spark/bin/spark-submit`) kullanmak gerekiyor.

---

## 6. Trino'dan Doğrulama

```bash
# Trino CLI'a bağlan:
docker exec -it odf-trino trino --catalog iceberg --schema odf_bronze

# Trino prompt'unda:
SELECT * FROM sales LIMIT 10;

# Çıkmak için:
exit
```

---

## 7. Superset'te Görselleştirme

1. `http://63.181.138.9:8088` adresine git, `admin` / `admin` ile giriş yap.
2. **SQL Lab → SQL Editor**: Database = Trino, Schema = `odf_bronze`, test sorgusu çalıştır.
3. **Settings → Datasets → + Dataset**: Database=Trino, Schema=`odf_bronze`, Table=`sales`.
4. Dataset'ten chart oluştur (Table veya Bar Chart), dashboard'a ekle.

> ⚠️ "Row limit reached" uyarısı çıkarsa, chart'ın **Row limit** ayarını veri boyutuna göre artır (örn. `50000`) — veri yanlış değil, sadece görünüm kısmi.

---

## 8. Airflow Kurulumu (Phase 2 — Orkestrasyon, devam ediyor)

### 8.1 Custom Dockerfile (`Dockerfile.airflow`)
```dockerfile
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
```

### 8.2 Dosyayı EC2'ye yükle
```bash
# Windows'ta:
cd C:\Users\yeliz\Desktop\Projeler\GitHub\VideoConference
scp -i nexmeet.pem C:\Users\yeliz\Downloads\Dockerfile.airflow ubuntu@63.181.138.9:~/odf-deploy/
```

### 8.3 DAGs klasörü oluştur
```bash
mkdir -p ~/odf-deploy/dags
```

### 8.4 docker-compose.yml'e servis ekle
`~/odf-deploy/docker-compose.yml` dosyasındaki `services:` bloğuna (diğer servislerle aynı girinti seviyesinde, `networks:` bloğundan önce) eklendi:
```yaml
  airflow:
    build:
      context: .
      dockerfile: Dockerfile.airflow
    container_name: odf-airflow
    user: "0:0"
    ports:
      - "8090:8080"
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
      - AWS_REGION=eu-central-1
    volumes:
      - ./airflow-data:/opt/airflow
      - ./dags:/opt/airflow/dags
      - /var/run/docker.sock:/var/run/docker.sock
    command: standalone
    restart: unless-stopped
```

> ⚠️ Dikkat: YAML girintilemesine dikkat — `airflow:` diğer servislerle (`trino:`, `spark:`, `nifi:` vb.) aynı 2 boşluk seviyesinde olmalı, bir önceki servisin (`nifi:`) içine yanlışlıkla girintilenmemeli.

### 8.5 Build ve başlat
```bash
cd ~/odf-deploy
docker compose up -d --build airflow
```

### 8.6 Doğrulama
```bash
docker ps
sleep 20
docker logs odf-airflow 2>&1 | grep -A 2 "Login with"
```

Airflow UI: `http://63.181.138.9:8090`

**Devam edecek:** NiFi'ı REST API ile tetikleyip ardından Spark job'ını (`docker exec` üzerinden) çalıştıran bir DAG yazılacak.

---

## Erişim Bilgileri Özeti

| Servis | URL | Kullanıcı |
|---|---|---|
| NiFi | `https://odf-nifi.local:8443` | admin / odfNifiPass123! |
| Superset | `http://63.181.138.9:8088` | admin / admin |
| Trino | `http://63.181.138.9:8080` | — |
| Airflow | `http://63.181.138.9:8090` | (loglardan alınır) |
