CREATE TABLE transaction (
    transactionId VARCHAR PRIMARY KEY,
    productId VARCHAR,
    productName VARCHAR,
    productCategory VARCHAR,
    productPrice NUMERIC(10, 2),
    productQuantity INT,
    productBrand VARCHAR,
    currency VARCHAR,
    customerId VARCHAR,
    transactionDatetime TIMESTAMP,
    paymentMethod VARCHAR,
    totalAmount NUMERIC(10, 2)
);

CREATE TABLE order_by_product (
    event_hour TIMESTAMP,
    productCategory VARCHAR,
    num_orders BIGINT,
    total_amount NUMERIC
);

CREATE TABLE order_by_window (
    event_hour TIMESTAMP,
    num_orders BIGINT,
    total_amount NUMERIC
);