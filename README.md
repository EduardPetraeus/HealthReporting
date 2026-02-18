# health_reporting

This repository contains tools and scripts for personal health data analysis.

## project_purpose
The goal of this project is to collect, process, and analyze health data from multiple sources to gain insights into physical activity, nutrition, and biomarkers.

## repository_structure
The project is divided into two main areas:

### 01_legacy_on_premise_dw
This folder contains a traditional SQL Server solution:
* **datawarehouse**: Contains database projects, integration projects, and packages for data movement and storage.
* **tabular_model**: Contains the analytical models used for reporting.

### 02_health_unified_platform
This is the new data platform designed to be technology-independent:
* **health_platform**: Contains scripts and logic for modern data processing and transformations.
* **health_environment**: Contains deployment configurations and environment settings.

## data_sources
The platform is designed to integrate data from several sources, including:

* **apple_health**: A comprehensive dataset covering activity, vitality, mobility, body metrics, sleep, nutrition, hygiene, and environmental audio exposure.
* **lifesum**: Detailed nutrition and food intake logs, including product names and meal types.
* **gettested**: Lab results from blood tests and microbiome analysis.
* **oura**: Sleep quality, recovery metrics, and daily activity tracking.
* **strava**: Detailed workout, exercise, and GPS-based activity data.
* **withings**: Weight, body composition, blood pressure, and body temperature.