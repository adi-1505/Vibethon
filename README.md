# Vibethon
Data engineering Project
# Problem statement
Modern music streaming platforms generate high-volume, high-velocity, and heterogeneous data from multiple sources including user interactions, track metadata, artist profiles, and real-time streaming events. This data is typically ingested in diverse formats such as JSON, Parquet, and relational databases, making unified analytics, performance monitoring, and business intelligence extremely complex.
Traditional batch-oriented ETL workflows struggle to support:
Incremental data ingestion at scale
Schema consistency across evolving datasets
Real-time analytical readiness
Optimized transformation for dimensional modeling
Automated orchestration with fault tolerance
As a result, organizations face delayed insights, data quality issues, and inefficient analytics pipelines.
 .

# Objective

To design and implement a cloud-native, automated, and scalable ETL pipeline for music streaming analytics that:

Ingests multi-source streaming data incrementally

Cleanses and standardizes raw data into structured formats

Applies medallion architecture (Bronze → Silver → Gold)

Builds analytics-ready star schema models

Enables efficient querying for user behavior and streaming trends

Ensures pipeline reliability and extensibility

# Proposed Solution

<img width="2784" height="1438" alt="Gemini_Generated_Image_js2xtijs2xtijs2x" src="https://github.com/user-attachments/assets/50235487-5e94-4f23-a210-8d3e3d1957ec" />

The system integrates:

Automated ingestion pipelines for structured and semi-structured data

Transformation layers for normalization and enrichment

Dimensional modeling using fact and dimension tables

Cloud-based orchestration and scalable compute

leveraging modern data engineering practices with platforms such as Databricks and cloud infrastructure provided by Microsoft.

# Expected Outcomes

Near real-time analytics readiness

High data quality and schema consistency

Scalable pipeline for growing streaming data volumes

Optimized reporting on:

User engagement

Popular artists & tracks

Temporal streaming trends

# Impact

This solution addresses real-world challenges faced by digital media platforms by delivering a robust, automated, and production-grade data pipeline capable of supporting advanced analytics, business intelligence, and data-driven decision-making.

# File Structure
Vibethon_extracted/
└── vibethon/
    ├── publish_config.json

    ├── dataset/
    │   ├── Json_dynamic.json
    │   ├── Parquet_dynamic.json
    │   └── azure_sql.json

    ├── factory/                 (Azure Data Factory)
    │   ├── dfazureproject22.json
    │   └── sppotifydb.json

    ├── linkedService/
    │   ├── Azure_Sql.json
    │   └── datalake.json

    ├── pipeline/               (ADF pipelines)
    │   ├── incremetal_ingection.json
    │   └── incremetal_loop.json

    └── spotify_dab/            (Databricks Asset Bundle)

        ├── README.md
        ├── databricks.yml
        ├── pyproject.toml

        ├── jinja/
        │   ├── jinja_notebook.py
        │   └── Untitled Notebook 2025-12-11 01_10_16.py

        ├── resources/
        │   ├── sample_job.job.yml
        │   └── spotify_dab_etl.pipeline.yml

        ├── src/
        │   ├── silver/
        │   │   ├── silver_dim.py
        │   │   └── Untitled Notebook 2025-12-10 13_55_42.py
        │   │
        │   └── gold/
        │       └── dlt/
        │           ├── transformations/
        │           │   ├── DimArtist.py
        │           │   ├── DimDate.py
        │           │   ├── DimTrack.py
        │           │   ├── DimUser.py
        │           │   └── FactStream.py
        │           ├── utilities/
        │           │   └── utils.py
        │           └── explorations/
        │               └── sample_exploration.py

        ├── utils/
        │   └── transformation.py
        └── tests/
            ├── conftest.py
            └── sample_taxis_test.py
 ├── README.md

# Automated-ETL-Pipeline-for-Music-Streaming-Analytics-<img width="1919" height="895" alt="image" src="https://github.com/user-attachments/assets/d8c67f8a-c046-4c1a-93fe-320df9e51875" />
<img width="1201" height="493" alt="image" src="https://github.com/user-attachments/assets/50a5b44e-d8dc-4a34-9c4f-0c1199dee9a8" />
<img width="1538" height="504" alt="image" src="https://github.com/user-attachments/assets/b74c78ea-fce9-4508-9186-8836d12771b0" />
