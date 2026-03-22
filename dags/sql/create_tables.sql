-- create orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY,
    created_at TIMESTAMP,
    website_session_id VARCHAR(50),
    user_id INT,
    primary_product_id INT,
    items_purchased INT,
    price_usd NUMERIC(10, 2),
    cogs_usd NUMERIC(10, 2)
);

-- create order_items table
CREATE TABLE IF NOT EXISTS order_items(
    order_item_id INT PRIMARY KEY,
    created_at TIMESTAMP,
    order_id INT,
    product_id INT,
    is_primary_item INT,
    price_usd NUMERIC(10, 2),
    cogs_usd NUMERIC(10, 2)
);

-- create order_item_refund table
CREATE TABLE IF NOT EXISTS order_item_refunds(
    order_item_refund_id INT PRIMARY KEY,
    created_at TIMESTAMP,
    order_item_id INT,
    order_id INT,
    refund_amount_usd NUMERIC(10, 2)
);

-- create products table
CREATE TABLE IF NOT EXISTS products(
    product_id INT PRIMARY KEY,
    created_at TIMESTAMP,
    product_name VARCHAR(255)
);

-- create website_page_view table
CREATE TABLE IF NOT EXISTS website_page_views(
    website_pageview_id INT PRIMARY KEY,
    created_at TIMESTAMP,
    website_session_id INT,
    pageview_url VARCHAR(255)
);

-- create website_sessions table
CREATE TABLE IF NOT EXISTS website_sessions(
    website_session_id INT PRIMARY KEY,
    created_at TIMESTAMP,
    user_id INT,
    is_repeat_session INT,
    utm_source VARCHAR(100),
    utm_campaign VARCHAR(100),
    utm_content VARCHAR(100),
    device_type VARCHAR(50),
    http_referer VARCHAR(255)
);
