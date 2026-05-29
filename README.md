# Advanced-DB-2025

## [Local Notebook](./Local/AdvancedDataBase.ipynb):

How to run the notebook locally:

## Windows 10/11:

### Requirements:
---------------------
1. Python 3
2. Pyspark
3. Apache Hadoop
4. Apache Sedonna

### Set up:
-------------------
1. Download and install [Python](https://www.python.org/downloads/release/python-3119/). We will be using Python 3.11.9 for compatibility purposes.
Make sure to add python to Path.

2. Open command prompt and install pyspark with pip:

    ```
    pip install pyspark
    ```
    Download and extract the pyspark distribution from the [Apache Spark website](https://www.apache.org/dyn/closer.lua/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz) (we will be using pyspark-3.5.4 with hadoop 3).

    - After extracting the file, create the __SPARK_HOME__ system environment and add the __path\to\pyspark-3.5.4-bin-hadoop3__. 
    - Add __path\to\pyspark-3.5.4-bin-hadoop3\bin__ to __Path__.


3. Download [Apache Hadoop](https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz) and extract its files to your prefered path (we use version 3.3.6)
In the __System Environment Variables__ make the following additions:
    
    - Add __path\to\hadoop\bin__ to __Path__
    - Create a variable __HADOOP_HOME__ with value __path\to\hadoop__

4. Install Apache Sedona using pip:
    ```
    pip install apache-sedona
    ```
    We will need to download some files for it to work correctly:
    - [Sedona Spark Shaded jar](https://repo.maven.apache.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.7.0/sedona-spark-shaded-3.5_2.12-1.7.0.jar)
    - [Geotools Wrapper jar](https://repo.maven.apache.org/maven2/org/datasyslab/geotools-wrapper/1.7.0-28.5/geotools-wrapper-1.7.0-28.5.jar)

    We will add both of  these files to __path\to\spark-3.5.4-bin-hadoop3\jars__