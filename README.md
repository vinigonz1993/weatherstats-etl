# Project

## Installing dependencies
To install environment and the dependencies for the project in a pipenv environment run the command:

```
make setup
```

*Installing additional packages may be required*

## Database
The database used in this project is a PostgreSQL database and was created for study purposes, for this reason the ports are open.

## Database Connection
It is possible to connect to the database from localhost using the credentials in ```connection.py```

## ETL
The ETL process included in this project is explicitly separated in files:

```
- extract.py
- transform.py
- load.py
```

## Run the code
To run the python program, run the following command on the terminal after the setup
```
make run
```

Running the code may take some time as there are nearly 8.000 rows to be inserted into the fact table, plus the database setup, including some drops and create tables.

## Group Members
- João Carolino Caro
- Micky Mwiti
- Vinícius Gonzalez Caetano
- Yan Klein