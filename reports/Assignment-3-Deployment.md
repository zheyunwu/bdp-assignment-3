# Big data platform - Assignment 3 - Deployment

## Environment

1. Kafka

    ```shell
    docker-compose -f kafka-docker-compose.yml up -d

    docker exec -it [kafka_container_id] /bin/bash

    $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic tenant1.taxi

    $KAFKA_HOME/bin/kafka-topics.sh --list --zookeeper zookeeper:2181
    ```

2. Spark

    ```shell
    docker-compose -f spark-docker-compose.yml up -d
    ```

3. Go to /code directory, install necessary python packages:

   ```shell
   pip3 install -r requirements.txt
   ```

4. Run daas API server:

    ```shell
    python3 mysimbdp-daas.py
    ```

## Stream analytics

1. Fill in right server config in tenantstreamapp.py and tenant_data_producer.py

2. Make sure CSV file is in /data folder

3. Run tenantstreamapp

    ```shell
    python3 tenantstreamapp.py
    ```

4. Emulate producing data

    ```shell
    python3 tenant_data_producer.py
    ```
