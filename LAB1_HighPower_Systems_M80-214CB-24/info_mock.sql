-- 6.1. Общая информация о данных
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT sale_customer_id) as unique_customers,
    COUNT(DISTINCT sale_seller_id) as unique_sellers,
    COUNT(DISTINCT sale_product_id) as unique_products,
    COUNT(DISTINCT store_name) as unique_stores,
    COUNT(DISTINCT supplier_name) as unique_suppliers
FROM mock_data;

-- 6.2. Анализ покупателей
SELECT 
    customer_country,
    COUNT(*) as customer_count,
    AVG(customer_age) as avg_age,
    COUNT(DISTINCT customer_pet_type) as pet_types_count
FROM mock_data 
GROUP BY customer_country 
ORDER BY customer_count DESC;

-- 6.3. Анализ продуктов
SELECT 
    product_category,
    COUNT(*) as product_count,
    AVG(product_price) as avg_price,
    MIN(product_price) as min_price,
    MAX(product_price) as max_price,
    AVG(product_rating) as avg_rating
FROM mock_data 
GROUP BY product_category 
ORDER BY product_count DESC;

-- 6.4. Анализ продаж
SELECT 
    store_country,
    SUM(sale_total_price) as total_revenue,
    AVG(sale_total_price) as avg_sale_amount,
    COUNT(*) as total_sales
FROM mock_data 
GROUP BY store_country 
ORDER BY total_revenue DESC;

-- 6.5. Анализ поставщиков по странам
SELECT 
    supplier_country,
    COUNT(DISTINCT supplier_name) as supplier_count,
    COUNT(DISTINCT product_category) as categories_covered
FROM mock_data 
GROUP BY supplier_country 
ORDER BY supplier_count DESC;

-- 6.6. Проверка целостности данных
SELECT 
    COUNT(*) as null_customer_count
FROM mock_data 
WHERE customer_first_name IS NULL OR customer_email IS NULL;

SELECT 
    COUNT(*) as null_product_count
FROM mock_data 
WHERE product_name IS NULL OR product_category IS NULL;