
# Streaming Data and Data Lakehouse Architectures

Start containers

```bash
docker compose up -d
```

Open UIs:

- HDFS NameNode: http://localhost:9870
- Spark History Server: http://localhost:18080
- Kafka UI: http://localhost:8090


## Postgres -> Kafka

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


## Spark usage examples

Sprk submit

```bash
docker exec -it spark-submit bash -lc \
  '/opt/spark/bin/spark-submit \
    /app/test_hdfs.py'
```
