# Construction of the **Song Play Database**

This repository sets for the codes that setup the Song Play Database. It is a database that includes multiple tables such as user, song, artist, song, and song play. It allows us to query into these tables and derive unique insights for this online song playing platform. An Entity-Relationship Diagram (ERN) that illustrates the relationships between the fact and the dimension tables is presented below:

![ERN diagram](https://github.com/flyersworder/udacity-data-model-project/blob/master/ERD.png)

As for the codes,

* *sql_queries.py* contains the SQL queries that generate the tables in the database and insert the data into these tables. Specifically, it creates the aforementioned tables and inserts data into these tables from the log files and the song files.
* *create_tables.py* generates a SQL database and creates the tables accordingly using the queries in the preceding file.
* *etl.py* automates the extract, transform, and load process using various functions and methods.

## Getting Started

The starting point is the queries. I simply write down the queries to create the tables and to insert the data into these tables, row by row. Since the codes to create the tables are already provided, these tables can easily be created. But for the table creation, I need to specify the constraints for different variables, e.g., the primary keys and the NOT NULL settings for these unique keys. I also need to take care of duplicate values during inserting. For external libraries, we particularly rely on the [psycopg2](https://pypi.org/project/psycopg2/) package to connect to the PostgreSQL database we created. You can install this package by running

```cmd
pip install psycopg2
```

in your terminal.

## Further improvements

Copying the records row by row from the log file is a bit low efficient, and I find that there is a **copy_from** command from the *postgresql* module that can copy data in blocks. Using this method significantly improves the data loading time (half the time as before). However, the drawback is that it can't remove duplicates automatically. Therefore, I add temporary tables to remove the duplicates and to insert the distinct values for the **user** and the **time** tables. I also add try-exception pairs to make the codes more traceable and robust.

Another improvement is that I create unique IDs for the *songplay_id*. Using the library *hashlib*, I first create a unique hash key for each file path, and then join this key with the record id to create a unique id for each song play. This id can thus be used as the primary key for this table.

## Usage

We can use some popular dashboard tools to create dashboards out of this database, for example, [plotly](https://plot.ly), [Apache Superset](https://superset.incubator.apache.org), or [streamlit](https://streamlit.io).
