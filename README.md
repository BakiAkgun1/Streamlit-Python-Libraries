# Database Analysis with Pandas, Dask, Modin, Vaex, and Polars

This project provides solutions to 15 different database problems using various Python libraries, including Pandas, Dask, Modin, Vaex, and Polars. Each library has its own page where the solutions to the problems are presented. The project also includes a user-friendly interface built with Streamlit, where users can explore and compare the performance of these libraries.


![python](https://github.com/user-attachments/assets/796ca908-b08f-4cb9-9775-62017e9e8959)

## Table of Contents

- [Project Overview](#project-overview)
- [Libraries Used](#libraries-used)
- [Installation](#installation)
- [How to Run the Project](#how-to-run-the-project)
- [Examples and Screenshots](#examples-and-screenshots)
- [About](#about)

## Project Overview

This project is designed to demonstrate the use of various Python libraries for handling large datasets and performing complex queries efficiently. It covers a wide range of operations, including data loading, merging, aggregation, and performance comparison among the libraries.

### Data Source

The data used in this project is pulled from the `sqlite_sakila.db` database, which is included in the main directory of the project. This database is modeled after the Sakila sample database, commonly used for educational purposes. It contains information about film rentals, customers, payments, and more, providing a rich dataset for analysis.

### View Database Records

An additional feature in the project allows you to view the first 10 records of each table in the database directly within the Streamlit interface. This makes it easier to understand the structure and contents of the database before diving into more complex analyses.

## Libraries Used

The project utilizes the following libraries:

- **Pandas**: A fast, powerful, and flexible data analysis and manipulation library.
- **Dask**: Parallel computing with task scheduling.
- **Modin**: An optimized Pandas-like library that scales efficiently across multiple CPUs.
- **Vaex**: Out-of-core DataFrames for working with large datasets, enabling fast queries and statistics.
- **Polars**: A fast, multi-threaded DataFrame library for Rust and Python.

Each of these libraries is introduced and compared on the main page of the Streamlit application, providing users with insights into when and why to use each one.

## Installation

To install the required libraries, simply run the following command in your terminal:

```bash
pip install -r requirements.txt
```

This will install all the necessary dependencies for running the project.

## How to Run the Project

After installing the dependencies, you can start the Streamlit application by running the following command:

```bash
streamlit run app.py
```

This will launch the main page of the project, where you can explore different database solutions implemented with the mentioned libraries.

## Examples and Screenshots
![Screenshot 2024-08-30 001155](https://github.com/user-attachments/assets/c4e170bb-760a-422d-ad15-5cd366a25695)
![Screenshot 2024-08-30 001228](https://github.com/user-attachments/assets/e462270d-9ae2-4a94-902f-4f063ef42271)

![Screenshot 2024-08-30 001208](https://github.com/user-attachments/assets/1464a154-0088-4120-9606-195fee467f99)

(Screenshots should be stored in a `screenshots` folder within the project directory.)

## About

This project was developed during my internship at Lotus AI. It showcases my skills in Python programming, data analysis, and library optimization, as well as my ability to create user-friendly interfaces for data-driven projects. The data for the analysis was extracted from the `sqlite_sakila.db` database, which provides a rich dataset for performing various data manipulation and analysis tasks.
