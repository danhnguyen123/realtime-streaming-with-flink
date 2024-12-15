from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.udf import ScalarFunction, udf
import os
import json
import requests
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment

def create_processed_events_sink_kafka(t_env: StreamTableEnvironment):
    table_name = "process_events_kafka"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            transactionId STRING,
            productId STRING,
            productName STRING,
            productCategory STRING,
            productPrice DOUBLE,
            productQuantity INT,
            productBrand STRING,
            currency STRING,
            customerId STRING,
            transactionDatetime TIMESTAMP,
            paymentMethod STRING,
            totalAmount DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'broker:29092',
            'topic' = 'financial_transactions_sink',
            'properties.group.id' = 'flink-group',
            'format' = 'json'
        );
        """
    print(sink_ddl)
    t_env.execute_sql(sink_ddl)
    return table_name

def create_processed_events_sink_elastic_search(t_env: StreamTableEnvironment):
    table_name = "process_events_elastic_search"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            transactionId STRING,
            productId STRING,
            productName STRING,
            productCategory STRING,
            productPrice DOUBLE,
            productQuantity INT,
            productBrand STRING,
            currency STRING,
            customerId STRING,
            transactionDate STRING,
            paymentMethod STRING,
            totalAmount DOUBLE,
            PRIMARY KEY (transactionId) NOT ENFORCED
        ) WITH (
            'connector' = 'elasticsearch-7',
            'hosts' = 'http://elasticsearch:9200',
            'index' = 'transactions'
        );
        """
    print(sink_ddl)
    t_env.execute_sql(sink_ddl)
    return table_name

def create_processed_events_sink_postgres(t_env: StreamTableEnvironment):
    table_name = 'processed_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            transactionId STRING,
            productId STRING,
            productName STRING,
            productCategory STRING,
            productPrice DOUBLE,
            productQuantity INT,
            productBrand STRING,
            currency STRING,
            customerId STRING,
            transactionDatetime TIMESTAMP,
            paymentMethod STRING,
            totalAmount DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'transaction',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env: StreamTableEnvironment):
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSSSSS"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            transactionId STRING,
            productId STRING,
            productName STRING,
            productCategory STRING,
            productPrice DOUBLE,
            productQuantity INT,
            productBrand STRING,
            currency STRING,
            customerId STRING,
            transactionDate VARCHAR,
            transactionDatetime AS TO_TIMESTAMP(transactionDate, '{pattern}'),
            paymentMethod STRING,
            totalAmount DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'broker:29092',
            'topic' = 'financial_transactions',
            'properties.group.id' = 'flink-group',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """
    print(source_ddl)
    t_env.execute_sql(source_ddl)
    return table_name

def log_processing():
    print('Starting Job!')
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    print('got streaming environment')
    env.enable_checkpointing(10)
    env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_processed_events_sink_postgres(t_env)
        elastic_search_sink = create_processed_events_sink_elastic_search(t_env)
        print('loading into postgres')
        t_env.execute_sql(
            f"""
                    INSERT INTO {postgres_sink}
                    SELECT
                        transactionId,
                        productId,
                        productName,
                        productCategory,
                        productPrice,
                        productQuantity,
                        productBrand,
                        currency,
                        customerId,
                        transactionDatetime,
                        paymentMethod,
                        totalAmount
                    FROM {source_table}
                    """
        )
        
        t_env.execute_sql(
            f"""
                    INSERT INTO {elastic_search_sink}
                    SELECT
                        transactionId,
                        productId,
                        productName,
                        productCategory,
                        productPrice,
                        productQuantity,
                        productBrand,
                        currency,
                        customerId,
                        transactionDate,
                        paymentMethod,
                        totalAmount
                    FROM {source_table}
                    """
        )
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

if __name__ == '__main__':
    log_processing()