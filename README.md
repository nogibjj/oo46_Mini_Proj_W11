[![CI](https://github.com/nogibjj/oo46_Mini_Proj_W11/actions/workflows/actions.yml/badge.svg)][def]

# Mini Project Week 11

## Project Scope

The mini project presents a data pipeline designed on the Microsoft Azure Databricks platform, focusing on the utilization of a sales_data_sample.csv file for demonstration purposes. The process commences with the uploading of the CSV file onto the Databricks File System (DBFS), setting the foundation for data ingestion. This crucial phase involves establishing a schema that defines the columns and their corresponding data types, ensuring the dataset's structural integrity when constructing a table within the Hive metastore.

Upon the successful ingestion of raw data into the designated table, the pipeline transitions towards the generation of subsidiary tables tailored for analytical explorations. This structured approach facilitates targeted data analysis and supports the extraction of insightful information.

Culminating the data processing journey, the project leverages these analytical tables to materialize data visualizations. These visualizations serve as a potent tool to encapsulate the derived insights, presenting them in an intuitive and accessible manner, thus enabling informed decision-making based on the processed data.

## Mini Project Deliverables:

[A Data Pipeline Notebook](https://github.com/nogibjj/oo46_Mini_Proj_W11/blob/main/Data_Pipeline.ipynb) with cells that perform a series of data processing taks:

### Data Ingestion

![ingest](reports/code.png)

### Creation of Analysis Tables

![tables](reports/code1.png)

### Data Visualizations

![sales](reports/output.png)

### Data Pipeline Summary

![summary](reports/output1.png)

### Job Run Analytics

![analytics](reports/analytics.png)

[def]: https://https://github.com/nogibjj/oo46_Mini_Proj_W11/actions/workflows/actions.yml
