
# Streaming Data and Data Lakehouse Architectures

This repository contains the code for the "Streaming Data and Data Lakehouse Architectures" workshop. 


# Prerequisites

Clone this repository and navigate to the `zhaw-zurich-spark` folder.


If you don't have Docker installed, you can download it from [official website](https://www.docker.com/products/docker-desktop).

You need Docker and Docker Compose to run the workshop.

### On Windows

If you are using Windows, you need to install [WSL2](https://docs.microsoft.com/en-us/windows/wsl/install-win10) 
and [Docker Desktop](https://www.docker.com/products/docker-desktop).

Check that you have WSL2 installed, open the search bar and type `Ubuntu` and press Enter. You should see the Ubuntu terminal.

Check the docker version with the following command:

```bash
docker version
```

Check the docker-compose version with the following command:

```bash
docker compose version
```

If you don't have Docker Compose installed, you can install it with the following command:

```bash
sudo apt-get install docker docker-compose
```

On Windows WSL2 to run docker commands you need to start a docker daemon in background.
Open a separate terminal and run the following command and keep it running:

```bash
sudo dockerd
```



### On Mac

If you are using Mac, you can install Docker Desktop from the [official website](https://www.docker.com/products/docker-desktop).

Check the docker version with the following command:

```bash
docker version
```

Check the docker-compose version with the following command:

```bash
docker compose version
```


## Part 1. Start the containers


Start containers

```bash
docker compose up -d
```

Open UIs:

- HDFS NameNode: http://localhost:9870
- Spark History Server: http://localhost:18080
- Kafka UI: http://localhost:8090


### Postgres -> Kafka

Login to Postgres database with the following command:

```bash
docker compose exec postgres psql -U postgres
```

You will see the Postgres prompt where you can run SQL queries.

```
psql (15.8 (Debian 15.8-1.pgdg120+1))
Type "help" for help.

postgres=#
```

Copy from the `postgres/insert-first-customers.sql` file and paste it into the 
terminal to insert some data into the tables. Press Enter to run query and then 
type `\q` to exit the Postgres terminal.

To see the created topics in Kafka, open the Kafka UI at http://localhost:8090.
Go to the *Topics* tab where you should see the `postgres.public.customers` and 
`postgres.public.orders` topics. Open a topic and click on the *Messages* tab to 
see the messages in JSON format.


### Simple Spark Job

Spark submit example:

```bash
docker exec -it spark-submit bash -lc \
  '/opt/spark/bin/spark-submit \
    /app/test_hdfs.py'
```


## PySpark Streaming Job: Retain customers

```bash
docker exec -it spark-submit bash /app/run_streaming.sh streaming_retain_customers.py
```
