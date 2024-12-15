import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcExecutionOptions, JdbcConnectionOptions
from pyflink.common import WatermarkStrategy
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("broker:29092") \
        .set_topics("financial_transactions") \
        .set_group_id("flink-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .build()

    # Set watermark strategy to no watermarks
    watermark_strategy = WatermarkStrategy.no_watermarks()

    transaction_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=watermark_strategy,
        source_name="Kafka Source"
    )

    # Define the type info corresponding to the structure of your dictionary
    type_info = Types.ROW([
        Types.STRING(),       # transactionId
        Types.STRING(),       # productId
        Types.STRING(),       # productName
        Types.STRING(),       # productCategory
        Types.DOUBLE(),       # productPrice
        Types.INT(),          # productQuantity
        Types.STRING(),       # productBrand
        Types.STRING(),       # currency
        Types.STRING(),       # customerId
        Types.SQL_TIMESTAMP(),# transactionDate
        Types.STRING(),       # paymentMethod
        Types.DOUBLE()        # totalAmount
    ])

    # Process JSON data
    def process_transaction(transaction):
        transaction = json.loads(transaction)
        return (
            transaction['transactionId'],
            transaction['productId'],
            transaction['productName'],
            transaction['productCategory'],
            transaction['productPrice'],
            transaction['productQuantity'],
            transaction['productBrand'],
            transaction['currency'],
            transaction['customerId'],
            transaction['transactionDate'],  # Convert to SQL_TIMESTAMP if needed
            transaction['paymentMethod'],
            transaction['totalAmount']
        )

    parsed_stream = transaction_stream.map(lambda transaction: process_transaction(transaction), output_type=type_info)

    # Print Transactions
    parsed_stream.print()

    # Create JDBC connection options
    jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
    username = "postgres"
    password = "postgres"

    exec_options = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
            .with_url(jdbc_url) \
            .with_driver_name('org.postgresql.Driver') \
            .with_user_name(username) \
            .with_password(password) \
            .build()

    conn_options = JdbcExecutionOptions.builder() \
            .with_batch_interval_ms(1000) \
            .with_batch_size(200) \
            .with_max_retries(5) \
            .build()

    # Insert transactions into the database
    parsed_stream.add_sink(
        JdbcSink.sink(
            sql="""INSERT INTO transactions (
                transaction_id, 
                product_id, 
                product_name, 
                product_category, 
                product_price, 
                product_quantity, 
                product_brand, 
                currency, 
                customer_id, 
                transaction_date, 
                payment_method, 
                total_amount
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            type_info=type_info,
            jdbc_connection_options=exec_options,
            jdbc_execution_options=conn_options
        )
    ).name("Insert Transactions into PostgreSQL")

    # Execute Job
    env.execute("Flink Python Job")

if __name__ == "__main__":
    main()