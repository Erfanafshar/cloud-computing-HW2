# Multi-Node Hadoop Cluster Crime Data Analysis

This project implements a multi-node Hadoop cluster using VirtualBox virtual machines to analyze real-world incident data from the City of Chicago. The system uses Apache Hadoop (YARN, HDFS, and MapReduce) to process and aggregate crime report data.

## System Overview

Three virtual machines were configured using Ubuntu (versions 18.04 and 20.04) and connected to form a Hadoop cluster. The cluster consists of:

- **VM1**: NameNode and ResourceManager
- **VM2 and VM3**: DataNode and NodeManager

Apache Hadoop 3.2.1 was installed and configured to support distributed storage (HDFS) and parallel data processing (MapReduce).

## Features Implemented

- Created three virtual machines with appropriate CPU and memory allocations to simulate a Hadoop cluster.
- Installed and configured Hadoop on all machines using SSH key-based communication.
- Set up HDFS with replication and verified functionality through both command line and Web GUI.
- Configured and tested MapReduce functionality by running the default WordCount program.
- Implemented a custom MapReduce program in Java to count crime records per police district using a provided dataset.
- Downloaded Chicago crime datasets and uploaded relevant data to HDFS for distributed processing.
- Generated a clean CSV output summarizing the number of crime incidents per district.
- Used Web GUI to confirm status of DataNodes, HDFS capacity, and active nodes.
- All major steps were verified with CLI commands and screenshots, including file uploads, execution, and result retrieval.

## Dataset

- Source: Kaggle – Chicago Crime Datasets (2001 to 2017)
- File format: CSV
- Processing goal: Count number of incidents per police district and output results as a numeric CSV file (`crime_count_per_district.csv`)

## Tools and Technologies

- Apache Hadoop 3.2.1 (HDFS, YARN, MapReduce)
- Java (MapReduce implementation)
- VirtualBox (multi-node simulation)
- Ubuntu Linux (18.04 and 20.04)
- Bash CLI, SSH
- HDFS Web Interface

