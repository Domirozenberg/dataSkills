# About This Project

Welcome to the **Data Skills** Project!

This initiative is designed to provide a hands-on, practical learning experience for individuals aspiring to build a career in the dynamic and ever-evolving field of data and practice to master SQL syntax in the context of realistic business questions.

Our primary goal is to equip you with foundational and advanced SQL skills through real-world business scenarios, leveraging a dedicated PostgreSQL database.

While SQL is the initial focus, this project aims to be a stepping stone towards a broader understanding of data concepts, tools, and technologies crucial for various data roles. This project will provide hands-on practice to master SQL syntax in the context of realistic business questions.

As the project evolves, we plan to cover other essential techniques and technologies relevant to data roles:
* **Data Modeling:** Principles of designing effective database schemas (e.g., Star Schema, Snowflake Schema). 
* **Data Visualization Tools:** Introduction to tools like Tableau, Power BI, Looker ... 
* **Python for Data Analysis:** Using Python with libraries like Pandas, NumPy for data manipulation and analysis. 
* **Docker for Data Environments:** Deep dive into containerization for reproducible data projects.

# Who Is This Project For?
* Individuals with little to no prior experience in data, looking for a structured learning path. 
* Students or professionals from other fields aiming to transition into data roles. 
* Anyone who wants to solidify their SQL knowledge with practical exercises and real-world data.

# Navigating the Data Field: Roles and Skills
The data field is vast, with various specializations. While there\'s often overlap, understanding the core responsibilities of each role can help you chart your career path. SQL is a fundamental skill across almost all of them.

## Data Engineer
Data Engineers are the architects and builders of the data infrastructure. They design, construct, install, test, and maintain data management systems. Their primary focus is on the availability,reliability, and efficiency of data pipelines. They work with large-scale data processing, ETL (Extract, Transform, Load) processes, and data warehousing solutions.

## BI Engineer (Business Intelligence Engineer) 
BI Engineers focus on transforming raw data into actionable insights for business decision-makers. They design and develop data models, dashboards, and reports using BI tools. Their goal is to make data accessible and understandable, enabling organizations to monitor performance, identify trends, and optimize operations.

## Data Analyst
Data Analysts are responsible for collecting, processing, and performing statistical analysis on data. They interpret data, analyze results, and provide ongoing reports, identifying patterns and trends to answer specific business questions. They often act as a bridge between data and business stakeholders.

## Data Scientist
Data Scientists are involved in more advanced analytical tasks. They use statistical methods, machine learning algorithms, and predictive modeling to extract deeper insights, build predictive models, and solve complex business problems. They often possess strong programming (e.g., Python, R) and statistical skills, in addition to SQL.

**In smaller companies or teams, it\'s common for a single role (often called a \"Data Analyst\" or even "BI Engineer") to encompass responsibilities from multiple areas described below. This means you might find a Data Analyst who also builds ETL pipelines, creates advanced dashboards, and even performs some predictive modeling. Being proficient in a breadth of skills (like SQL, Python, and BI tools) makes you highly adaptable and valuable in such environments.**

# Project Structure & Learning Path
This project is built around a PostgreSQL database (conveniently delivered via Docker) containing a dataset about the Paris 2024 Olympic Games. This allows you to practice SQL on relevant, real-world-like data.

##Dataset Overview
The dataset includes information covering various aspects of the Olympic Games, such as:
* **athletes.csv:** Personal information about all participating athletes.
* **coaches.csv:** Personal information about all coaches. 
* **events.csv:** Details of all scheduled events. 
* **medals.csv:** Information about individual medal winners. 
* **medals_total.csv:** Aggregated medal counts by country. 
* **medalists.csv:** List of all medalists. 
* **nocs.csv:** National Olympic Committees (country codes and names). 
* **schedule.csv:** Day-by-day schedule of events. 
* **schedule_preliminary.csv:** Preliminary event schedule. 
* **teams.csv:** Information about participating teams. 
* **technical_officials.csv:** Details on referees, judges, etc. 
* **results.csv:** All event results. 
* **torch_route.csv:** Information about the torch relay. 
* **venues.csv:** Details of all Olympic venues. 
* **res_xxx.csv files:** Games results

You will be using these tables to answer a wide array of business questions.

## Prerequisites

* **Python:** Download it from https://python.org
* **Git:** Download it from https://git-scm.com
* **Docker Desktop:** Ensure Docker Desktop is installed and running on your system (Windows, macOS, or Linux). Download it from https://docs.docker.com
* **A SQL Client:** Choose your preferred tool to connect to PostgreSQL: 
    * psql (Command Line): The native PostgreSQL command-line client. 
    * pgAdmin: A popular graphical administration and development tool for PostgreSQL. Download from https://pgadmin.org. 
    * DBeaver: A universal database tool supporting PostgreSQL and many others.Download from https://dbeaver.io. 

## Setup Instructions

1. Clone this repository
```
git clone <repository-url> 
cd <repository-directory>
```
2. Run the docker compose to init the Postgres's database
```
cd <repository-directory>/docker 
docker compose up
```

3. Populate the database
```
cd <repository-directory>/sqlLearning/src
python3 load_all_csv_to_postgres.py ../dataset
```

## Accessing the PostgreSQL Database

Once the container is up and running, you can connect to it using your
chosen SQL client with the following details: 
* **Host:** localhost (or 127.0.0.1) 
* **Port:** 5430 (the host port you mapped) 
* **Database:** sqlLearningDB 
* **Schema:** sources 
* **User:** postgres 
* **Password:** postgres

## How to Use This Project 

1. **Explore the Schema:** After connecting to the database, take time to explore the tables and their columns 
2. **Navigate SQL Challenges:** The <repository-directory>/sqlLearning/sqlChallenges directory contains files for each SQL level. 
3. **Solve Business Questions:** Each level will present specific business questions in .sql files. Your task is to write the SQL queries to answer them effectively. 
4. **Verify Your Answers:** Execute your queries and compare your results to the expected outcomes.
