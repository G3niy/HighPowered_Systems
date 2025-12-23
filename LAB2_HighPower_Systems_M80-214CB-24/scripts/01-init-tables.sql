-- 1. Витрина продаж по продуктам
CREATE TABLE IF NOT EXISTS db.top_10_products (
    product_id Int32,
    name String,
    category String,
    total_quantity Int64,
    total_revenue Decimal(18,2),
    sales_count Int64,
    rating Decimal(5,3),
    reviews Int64,
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (total_quantity, product_id)
PARTITION BY toYYYYMM(created_date);

CREATE TABLE IF NOT EXISTS db.category_revenue_mart (
    category String,
    category_revenue Decimal(18,2),
    category_total_quantity Int64,
    products_count Int64,
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (category, created_date)
PARTITION BY toYYYYMM(created_date);

CREATE TABLE IF NOT EXISTS db.product_ratings_mart (
    product_id Int32,
    name String,
    category String,
    rating Decimal(5,3),
    reviews Int64,
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (category, rating)
PARTITION BY toYYYYMM(created_date);


-- 2. Витрина продаж по клиентам (исправленная - без email)
CREATE TABLE IF NOT EXISTS db.customer_sales_mart (
    customer_id Int32,
    first_name String,
    last_name String,
    country String,
    total_spent Decimal64(2),
    purchase_count Int64,
    avg_order_value Decimal64(2),
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (country, customer_id, created_date);

CREATE TABLE IF NOT EXISTS db.top_10_customers (
    customer_id Int32,
    first_name String,
    last_name String,
    country String,
    total_spent Decimal64(2),
    purchase_count Int64,
    avg_order_value Decimal64(2),
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (total_spent, created_date);

CREATE TABLE IF NOT EXISTS db.customer_country_distribution (
    country String,
    customer_count Int64,
    total_spent_by_country Decimal64(2),
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (country, created_date);

-- 3. Витрина продаж по времени (исправленная - без unique_customers)
CREATE TABLE IF NOT EXISTS db.monthly_trends_mart (
    year Int32,
    month Int32,
    year_month String,
    monthly_revenue Decimal(18,2),
    monthly_orders Int64,
    avg_order_size_monthly Decimal(18,2),
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (year_month, year, month)
PARTITION BY toYYYYMM(created_date);

CREATE TABLE IF NOT EXISTS db.yearly_trends_mart (
    year Int32,
    yearly_revenue Decimal(18,2),
    yearly_orders Int64,
    avg_order_size_yearly Decimal(18,2),
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (year, created_date)
PARTITION BY toYYYYMM(created_date);

CREATE TABLE IF NOT EXISTS db.monthly_comparison_mart (
    month Int32,
    avg_monthly_revenue Decimal(18,2),
    avg_monthly_orders Decimal(18,2),
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (month, created_date)
PARTITION BY toYYYYMM(created_date);

-- 4. Витрина продаж по магазинам
CREATE TABLE IF NOT EXISTS db.store_sales_mart (
    store_id Int32,
    name String,
    city String,
    country String,
    total_revenue Decimal64(2),
    total_orders Int64,
    unique_customers Int32,
    total_quantity_sold Int32,
    avg_order_value Decimal64(2),
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (country, city, created_date);


CREATE TABLE IF NOT EXISTS db.top_5_stores (
    store_id Int32,
    name String,
    city String,
    country String,
    total_revenue Decimal64(2),
    total_orders Int64,
    unique_customers Int32,
    total_quantity_sold Int32,
    avg_order_value Decimal64(2),
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (total_revenue, created_date);

CREATE TABLE IF NOT EXISTS db.store_geo_distribution (
    country String,
    city String,
    total_revenue_by_location Decimal64(2),
    store_count Int64,
    total_orders_by_location Int64,
    avg_order_value_by_location Decimal64(6),
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (country, city, created_date);

-- 5. Витрина продаж по поставщикам (упрощенная)

CREATE TABLE IF NOT EXISTS db.supplier_sales_mart (
    supplier_id Int32,
    name String,
    country String,
    contact String,
    total_revenue Decimal64(2),
    total_sales Int64,
    unique_products_sold Int64,
    total_quantity_sold Int64,
    avg_product_price Decimal64(6),
    min_product_price Decimal64(2),
    max_product_price Decimal64(2),
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (country, supplier_id, created_date);

CREATE TABLE IF NOT EXISTS db.top_5_suppliers (
    supplier_id Int32,
    name String,
    country String,
    contact String,
    total_revenue Decimal64(2),
    total_sales Int64,
    unique_products_sold Int64,
    total_quantity_sold Int64,
    avg_product_price Decimal64(6),
    min_product_price Decimal64(2),
    max_product_price Decimal64(2),
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (total_revenue, created_date);

CREATE TABLE IF NOT EXISTS db.supplier_country_distribution (
    country String,
    total_revenue_by_country Decimal64(2),
    supplier_count Int64,
    total_sales_by_country Int64,
    avg_product_price_by_country Decimal64(10),
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (country, created_date);

-- 6. Витрина качества продукции
CREATE TABLE IF NOT EXISTS db.product_quality_mart (
    product_id Int32,
    name String,
    category String,
    rating Decimal64(1),
    reviews Int64,
    total_sold Int64,
    total_revenue Decimal64(2),
    sales_count Int64,
    avg_sale_price Decimal64(6),
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (category, rating, created_date);

CREATE TABLE IF NOT EXISTS db.highest_rated_products (
    product_id Int32,
    name String,
    category String,
    rating Decimal64(1),
    reviews Int64,
    total_sold Int64,
    total_revenue Decimal64(2),
    sales_count Int64,
    avg_sale_price Decimal64(6),
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (rating, created_date);

CREATE TABLE IF NOT EXISTS db.lowest_rated_products (
    product_id Int32,
    name String,
    category String,
    rating Decimal64(1),
    reviews Int64,
    total_sold Int64,
    total_revenue Decimal64(2),
    sales_count Int64,
    avg_sale_price Decimal64(6),
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (rating, created_date);

CREATE TABLE IF NOT EXISTS db.most_reviewed_products (
    product_id Int32,
    name String,
    category String,
    rating Decimal64(1),
    reviews Int64,
    total_sold Int64,
    total_revenue Decimal64(2),
    sales_count Int64,
    avg_sale_price Decimal64(6),
    created_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (reviews, created_date);

CREATE TABLE IF NOT EXISTS db.correlation_analysis_mart (
    corr_rating_sales Float64,
    corr_rating_revenue Float64,
    corr_rating_reviews Float64,
    analysis_date Date DEFAULT today()
) ENGINE = MergeTree()
ORDER BY (analysis_date);