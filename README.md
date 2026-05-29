# Advanced Databases — Big Data Crime Analytics on AWS

A distributed big data analytics pipeline that processes the [LA Crime Dataset (2010–2019)](https://data.lacity.org/Public-Safety/Crime-Data-from-2010-to-2019/63jg-8b9z) using **Apache Spark** on **AWS EMR**, with geospatial analysis powered by **Apache Sedona**. The project explores the full Spark ecosystem — comparing DataFrame, RDD, and SQL APIs — while benchmarking join strategies, data formats, and cluster scaling configurations.

> **Course:** Advanced Databases, National Technical University of Athens (NTUA)  
> **Semester:** 9th Semester, 2024–2025

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        AWS Cloud                            │
│                                                             │
│  ┌──────────┐      ┌──────────────────────────────────┐     │
│  │  Amazon  │      │         Amazon EMR Cluster        │     │
│  │    S3    │◄────►│                                    │     │
│  │          │      │  ┌────────┐  ┌────────┐           │     │
│  │ • CSV    │      │  │Executor│  │Executor│  ...      │     │
│  │ • Parquet│      │  │  (×4)  │  │  (×4)  │           │     │
│  │ • GeoJSON│      │  └────────┘  └────────┘           │     │
│  └──────────┘      │       Apache Spark 3.5.4          │     │
│                    │       Apache Sedona 1.7.0          │     │
│                    │       PySpark + Hadoop 3           │     │
│                    └──────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## Datasets

| Dataset | Description | Format |
|---------|-------------|--------|
| **Crime Data** | ~2.1M crime reports in Los Angeles (2010–2019) | CSV |
| **LA Police Stations** | 21 LAPD division stations with coordinates | CSV |
| **LA Income 2015** | Median household income by zip code | CSV |
| **2010 Census Blocks** | LA census block geometries with population data | GeoJSON |
| **RE Codes** | Victim descent/race code lookup table | CSV |

---

## Queries & Experiments

### Query 1 — Spark API Comparison (DataFrame vs RDD)
Counts aggravated assault victims by age group using both the **DataFrame API** and the **RDD API**, with execution time benchmarking.

**Key finding:** DataFrames leverage the Catalyst optimizer for automatic query optimization, resulting in **~18x faster** execution locally (4.2s vs 75.3s). On AWS EMR, the gap narrows due to distributed execution (5.4s vs 6.3s).

| API | Local Time | AWS Time |
|-----|-----------|----------|
| DataFrame | 4.23s | 5.36s |
| RDD | 75.28s | 6.30s |

---

### Query 2 — DataFrame vs SQL API + Parquet Optimization
Identifies the **top 3 police precincts by case closure rate** per year using window functions (`ROW_NUMBER() OVER PARTITION BY`). Implemented with both DataFrame and SQL APIs.

Also benchmarks **CSV vs Parquet** data format performance:

| Format | Local Time | AWS Time |
|--------|-----------|----------|
| CSV (DataFrame) | 4.97s | 4.38s |
| CSV (SQL) | 4.83s | 3.38s |
| **Parquet** (DataFrame) | **3.65s** | **3.91s** |

**Key finding:** Parquet's columnar storage and built-in compression consistently deliver **15–20% faster** query execution.

---

### Query 3 — Geospatial Analysis + Join Strategy Benchmarking
Correlates **income per person with crime rate per community** by performing a spatial join between crime GPS coordinates and census block polygons using **Apache Sedona's `ST_Within`**.

Benchmarked **4 different join strategies** using Spark hints and `explain()`:

| Join Strategy | Local Time | Physical Plan |
|---------------|-----------|---------------|
| Base (Auto) | 35.44s | RangeJoin + BroadcastHashJoin |
| **Broadcast** | **20.25s** | BroadcastIndexJoin + R-Tree |
| Merge | 30.78s | RangeJoin + ShuffledMergeJoin |
| Shuffle Hash | 31.43s | RangeJoin + ShuffledHashJoin |
| Shuffle Replicate NL | 29.75s | RangeJoin + CartesianProduct |

**Key finding:** **Broadcast join** was optimal because the income/census tables are small enough to be sent to all executors, avoiding expensive shuffle operations. Sedona's R-Tree spatial index further accelerated the spatial join.

---

### Query 4 — Cluster Resource Scaling
Analyzes **victim demographics by descent** in the 3 highest and 3 lowest income communities (2015 data). Runs the same query across different executor configurations to study resource scaling:

| Configuration | Executors | Cores/Executor | Memory/Executor |
|--------------|-----------|----------------|-----------------|
| Small | 2 | 1 | 2 GB |
| Medium | 2 | 2 | 4 GB |
| Large | 2 | 4 | 8 GB |

---

## Tech Stack

| Technology | Version | Purpose |
|-----------|---------|---------|
| **Apache Spark** | 3.5.4 | Distributed data processing |
| **PySpark** | 3.5.4 | Python API for Spark |
| **Apache Sedona** | 1.7.0 | Geospatial queries & spatial indexing |
| **Apache Hadoop** | 3.3.6 | Distributed storage layer |
| **AWS EMR** | — | Managed Spark cluster |
| **Amazon S3** | — | Cloud object storage |
| **Python** | 3.11.9 | Primary language |

---

## Project Structure

```
├── Local/
│   └── AdvancedDataBase.ipynb       # Full pipeline — runs locally
├── AWS/
│   └── AdvancedDataBaseAWS.ipynb    # Full pipeline — runs on AWS EMR
├── Data/
│   ├── Crime_Data_from_2010_to_2019_20241101.csv
│   ├── LA_Police_Stations.csv
│   ├── LA_income_2015.csv
│   ├── RE_codes.csv
│   └── 2010_Census_Blocks.geojson
└── README.md
```

---

## Getting Started

### Option A: Run on AWS EMR

1. **Create an EMR cluster** with Spark 3.5.4 and upload your notebooks
2. **Upload data files** to an S3 bucket
3. Update the S3 paths in `AdvancedDataBaseAWS.ipynb`
4. Run the notebook via **JupyterHub** on EMR

### Option B: Run Locally (Windows 10/11)

#### Prerequisites

| Software | Version | Notes |
|----------|---------|-------|
| Python | 3.11.9 | [Download](https://www.python.org/downloads/release/python-3119/) — add to PATH |
| Apache Spark | 3.5.4 | [Download](https://www.apache.org/dyn/closer.lua/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz) |
| Apache Hadoop | 3.3.6 | [Download](https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz) |

#### Step 1 — Install Python packages
```bash
pip install pyspark apache-sedona
```

#### Step 2 — Configure Spark
Extract the Spark distribution and set environment variables:
- Create `SPARK_HOME` → `path\to\spark-3.5.4-bin-hadoop3`
- Add `%SPARK_HOME%\bin` to `PATH`

#### Step 3 — Configure Hadoop
Extract Hadoop and set environment variables:
- Create `HADOOP_HOME` → `path\to\hadoop-3.3.6`
- Add `%HADOOP_HOME%\bin` to `PATH`

#### Step 4 — Install Sedona JARs
Download and place in `%SPARK_HOME%\jars\`:
- [sedona-spark-shaded-3.5_2.12-1.7.0.jar](https://repo.maven.apache.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.7.0/sedona-spark-shaded-3.5_2.12-1.7.0.jar)
- [geotools-wrapper-1.7.0-28.5.jar](https://repo.maven.apache.org/maven2/org/datasyslab/geotools-wrapper/1.7.0-28.5/geotools-wrapper-1.7.0-28.5.jar)

#### Step 5 — Run
Open `AdvancedDataBase.ipynb` in Jupyter Notebook and execute cells sequentially.

---

## Key Takeaways

- **DataFrame/SQL APIs outperform RDD** due to Catalyst optimizer's automatic query planning
- **Parquet format** provides consistent speed improvements over CSV through columnar storage and predicate pushdown
- **Broadcast joins** are optimal when one side of the join is small enough to fit in executor memory
- **Geospatial indexing** (R-Tree via Sedona) is critical for performant spatial joins on large datasets
- **Resource scaling** shows diminishing returns — doubling cores/memory doesn't halve execution time due to serialization and coordination overhead
