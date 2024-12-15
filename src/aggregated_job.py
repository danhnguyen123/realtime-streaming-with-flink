import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Tumble


def create_aggregated_events_sink_postgres(t_env: StreamTableEnvironment):
    table_name = 'processed_events_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            event_hour TIMESTAMP,
            productCategory VARCHAR,
            num_orders BIGINT,
            total_amount DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'order_by_product',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_aggregated_events_referrer_sink_postgres(t_env: StreamTableEnvironment):
    table_name = 'processed_events_aggregated_source'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            event_hour TIMESTAMP,
            num_orders BIGINT,
            total_amount DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'order_by_window',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_processed_events_source_kafka(t_env: StreamTableEnvironment):
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSSSSS"
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
            transactionDate VARCHAR,
            paymentMethod STRING,
            totalAmount DOUBLE,
            window_timestamp AS TO_TIMESTAMP(transactionDate, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'broker:29092',
            'topic' = 'financial_transactions',
            'properties.group.id' = 'flink-group-agg',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10)
    env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka table
        source_table = create_processed_events_source_kafka(t_env)

        aggregated_table = create_aggregated_events_sink_postgres(t_env)
        aggregated_sink_table = create_aggregated_events_referrer_sink_postgres(t_env)

        t_env.from_path(source_table)\
            .window(
            Tumble.over(lit(5).minutes).on(col("window_timestamp")).alias("w")
        ).group_by(
            col("w"),
            col("productCategory")
        ) \
            .select(
                    col("w").start.alias("event_hour"),
                    col("productCategory"),
                    col("transactionId").count.alias("num_orders"),
                    col("totalAmount").sum.alias("total_amount")
            ) \
            .execute_insert(aggregated_table)

        t_env.from_path(source_table).window(
            Tumble.over(lit(5).minutes).on(col("window_timestamp")).alias("w")
        ).group_by(
            col("w"),
        ) \
            .select(
                    col("w").start.alias("event_hour"),
                    col("transactionId").count.alias("num_orders"),
                    col("totalAmount").sum.alias("total_amount")
        ) \
            .execute_insert(aggregated_sink_table)

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    main()