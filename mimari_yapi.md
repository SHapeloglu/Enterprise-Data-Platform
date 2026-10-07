# Open Data Fabric — Apache Ağırlıklı Referans Mimari

> Amaç: Microsoft Fabric'teki temel deneyimleri mümkün olduğunca Apache/açık kaynak bileşenlerle karşılayan, on-prem / cloud / hybrid çalışabilen birleşik veri platformu.

## 1. Üst Seviye Mimari

```mermaid
flowchart TB
    U[👤 Kullanıcı / Data Engineer / BI Developer]

    subgraph ACCESS["Erişim ve Platform Deneyimi"]
        PORTAL["Unified Portal / Workspace Shell\nÖzel geliştirme"]
        KC["Keycloak\nIdentity • SSO • OIDC"]
        KNOX["Apache Knox\nSecure Gateway"]
    end

    subgraph META["Workspace • Catalog • Governance"]
        GRAV["Apache Gravitino\nMetalake • Catalog • Metadata"]
        ATLAS["Apache Atlas\nLineage • Classification • Governance"]
    end

    subgraph BUILD["Data Engineering ve Geliştirme"]
        NIFI["Apache NiFi\nIngestion • Pipeline"]
        HOP["Apache Hop Web\nVisual Dataflow / ETL"]
        ZEP["Apache Zeppelin\nNotebook"]
        AIR["Apache Airflow\nOrchestration"]
    end

    subgraph COMPUTE["Compute ve Real-Time"]
        SPARK["Apache Spark\nBatch / Distributed Compute"]
        KAFKA["Apache Kafka\nEvent Streaming"]
        FLINK["Apache Flink\nStream Processing"]
    end

    subgraph LAKE["Lakehouse"]
        ICE["Apache Iceberg\nOpen Table Format"]
        OZONE["Apache Ozone\nDistributed Object Storage"]
    end

    subgraph SERVE["SQL • Semantic • BI"]
        TRINO["Trino\nFederated SQL Endpoint"]
        KYLIN["Apache Kylin\nSemantic / OLAP Acceleration"]
        SUPER["Apache Superset\nBI • Dashboard • Visualization"]
    end

    subgraph OBS["Observability"]
        SKY["Apache SkyWalking\nMetrics • Logs • Traces • Alerts"]
    end

    U --> PORTAL
    PORTAL --> KC
    KC --> KNOX
    KNOX --> GRAV

    GRAV <--> ATLAS
    GRAV --> NIFI
    GRAV --> HOP
    GRAV --> ZEP
    GRAV --> TRINO
    GRAV --> KYLIN

    AIR -. orchestrates .-> NIFI
    AIR -. orchestrates .-> HOP
    AIR -. orchestrates .-> ZEP
    AIR -. orchestrates .-> SPARK

    NIFI --> ICE
    HOP --> ICE
    ZEP --> SPARK
    SPARK <--> ICE
    ICE <--> OZONE

    NIFI --> KAFKA
    KAFKA --> FLINK
    FLINK --> ICE

    TRINO <--> ICE
    KYLIN <--> ICE
    TRINO --> SUPER
    KYLIN --> SUPER

    SKY -. observes .-> NIFI
    SKY -. observes .-> HOP
    SKY -. observes .-> AIR
    SKY -. observes .-> SPARK
    SKY -. observes .-> KAFKA
    SKY -. observes .-> FLINK
    SKY -. observes .-> TRINO
    SKY -. observes .-> KYLIN
    SKY -. observes .-> SUPER
```

## 2. Fabric ile Katman Eşleştirmesi

```mermaid
flowchart LR
    subgraph FABRIC["Microsoft Fabric"]
        FP[Fabric Portal / Workspace]
        FC[OneLake Catalog]
        FL[Lakehouse]
        FD[Delta Lake]
        FO[OneLake]
        FPIPE[Data Pipeline]
        FDF[Dataflow Gen2]
        FNB[Notebook]
        FSP[Spark]
        FSQL[SQL Analytics Endpoint]
        FSM[Semantic Model]
        FBI[Power BI]
        FEV[Eventstream / RTI]
        FMON[Monitoring Hub]
    end

    subgraph OPEN["Open Data Fabric"]
        OP[Unified Portal + Gravitino Metalake]
        OG[Apache Gravitino]
        OL[Lakehouse Experience]
        OI[Apache Iceberg]
        OO[Apache Ozone]
        ON[Apache NiFi]
        OH[Apache Hop Web]
        OZ[Apache Zeppelin]
        OS[Apache Spark]
        OT[Trino]
        OK[Apache Kylin]
        OSU[Apache Superset]
        OEV[Apache Kafka + Flink]
        OM[Apache SkyWalking]
    end

    FP -.≈.-> OP
    FC -.≈.-> OG
    FL -.≈.-> OL
    FD -.≈.-> OI
    FO -.≈.-> OO
    FPIPE -.≈.-> ON
    FDF -.≈.-> OH
    FNB -.≈.-> OZ
    FSP -.=.-> OS
    FSQL -.≈.-> OT
    FSM -.≈.-> OK
    FBI -.≈.-> OSU
    FEV -.≈.-> OEV
    FMON -.≈.-> OM
```

## 3. Lakehouse İç Yapısı

```mermaid
flowchart TB
    G["Apache Gravitino\nCatalog / Metadata"]

    subgraph LH["Lakehouse"]
        B["Bronze / STG\nRaw + Incremental"]
        S["Silver / DWH\nClean • Conformed • Dim/Fact"]
        GD["Gold / Datamart\nServing-ready"]
    end

    I["Apache Iceberg\nSchema • Partition • Snapshot • Table Metadata"]
    O["Apache Ozone\nPhysical Data / Parquet Objects"]

    G --> B
    G --> S
    G --> GD
    B --> S --> GD
    B --> I
    S --> I
    GD --> I
    I --> O
```

## 4. Kullanıcı Deneyimi — Fabric Benzeri Portal

```text
┌──────────────────────────────────────────────────────────────────────┐
│                         OPEN DATA FABRIC                             │
├──────────────────────────────────────────────────────────────────────┤
│ Workspace: Sales Analytics                         User: selim       │
│                                                                      │
│  + New                                                               │
│                                                                      │
│  🗄 Lakehouse        → Gravitino + Iceberg + Ozone                   │
│  🔀 Pipeline         → Apache NiFi                                   │
│  🧩 Dataflow         → Apache Hop Web                                │
│  📓 Notebook         → Apache Zeppelin + Spark                       │
│  🧮 SQL              → Trino                                         │
│  🔷 Semantic Model   → Apache Kylin                                  │
│  📊 Report           → Apache Superset                               │
│  ⚡ Real-Time        → Apache Kafka + Flink                          │
│  🧬 Lineage          → Apache Atlas                                  │
│  📈 Monitor          → Apache SkyWalking                             │
└──────────────────────────────────────────────────────────────────────┘
```

## 5. Veri Akışı Örneği

```mermaid
flowchart LR
    SRC[(SQL Server / PostgreSQL / API / Files)]
    N[Apache NiFi]
    H[Apache Hop]
    B[Bronze]
    S[Silver]
    G[Gold]
    T[Trino]
    K[Apache Kylin]
    BI[Apache Superset]

    SRC -->|Ingest / Copy| N
    N --> B
    B -->|Visual ETL| H
    H --> S
    S -->|Spark / SQL Transform| G
    G --> T
    G --> K
    T --> BI
    K --> BI
```

## 6. Bileşenlerin Sorumlulukları

| Katman | Ürün | Ana sorumluluk |
|---|---|---|
| Portal | Özel geliştirme | Tek giriş noktası, workspace ve Fabric-benzeri UX |
| Identity | Keycloak | Kullanıcı, grup, SSO, OIDC/OAuth2 |
| Gateway | Apache Knox | Veri platformu servislerine güvenli erişim |
| Catalog | Apache Gravitino | Metalake, catalog, schema, table ve metadata omurgası |
| Governance | Apache Atlas | Lineage, classification ve governance |
| Storage | Apache Ozone | Dağıtık fiziksel object storage |
| Table Format | Apache Iceberg | ACID lakehouse tabloları ve metadata |
| Ingestion | Apache NiFi | Kaynaklardan veri taşıma ve akış tabanlı pipeline |
| Visual ETL | Apache Hop Web | Dataflow Gen2 benzeri görsel dönüşüm |
| Orchestration | Apache Airflow | DAG, dependency, schedule ve job orchestration |
| Notebook | Apache Zeppelin | Web notebook ve interaktif geliştirme |
| Compute | Apache Spark | Büyük ölçekli batch / distributed processing |
| SQL | Trino | Federated/ad-hoc SQL endpoint |
| Semantic / OLAP | Apache Kylin | Semantic/OLAP model ve query acceleration |
| BI | Apache Superset | Dashboard, rapor ve visualization |
| Event Streaming | Apache Kafka | Event ingestion / event bus |
| Stream Compute | Apache Flink | Stateful real-time processing |
| Observability | Apache SkyWalking | Metrics, logs, traces, alerts ve topology |

## 7. Tasarım İlkesi

Platformun merkezi yalnızca storage değildir:

```text
                    Unified Portal
                          │
                    Apache Knox
                          │
                  Apache Gravitino
                   (mantıksal omurga)
                          │
       ┌──────────────────┼──────────────────┐
       │                  │                  │
 Data Engineering       SQL / BI          Streaming
       │                  │                  │
       └──────────────────┼──────────────────┘
                          │
                    Apache Iceberg
                          │
                    Apache Ozone
                   (fiziksel omurga)
```

**Gravitino = mantıksal metadata/catalog omurgası**  
**Iceberg = lakehouse tablo katmanı**  
**Ozone = fiziksel veri omurgası**  
**Portal = bütün bileşenleri tek ürün deneyiminde birleştiren katman**

## 8. Fabric'e Göre En Önemli Fark

Microsoft Fabric'te portal, identity, catalog, storage, compute, semantic model ve BI tek yönetilen ürün deneyimi içinde gelir. Bu mimaride teknik motorların büyük bölümü hazır açık kaynak projelerden oluşur; bizim geliştirmemiz gereken temel değer **bu motorları tek workspace, ortak yetkilendirme, ortak metadata ve tutarlı kullanıcı deneyimi altında birleştirmektir.**

> Not: Trino ve Keycloak Apache Software Foundation projesi değildir; mevcut tasarımda teknik ihtiyaç nedeniyle bilinçli olarak korunmuştur.
