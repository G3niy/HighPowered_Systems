-- Заполнение измерения клиентов (убираем дубликаты по всем полям)
INSERT INTO dim_customers (first_name, last_name, age, email, country, postal_code)
SELECT DISTINCT 
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code
FROM mock_data
ON CONFLICT (first_name, last_name, email, age, country) DO NOTHING;

-- Заполнение измерения питомцев
INSERT INTO dim_pets (customer_id, pet_type, pet_name, pet_breed)
SELECT DISTINCT 
    c.customer_id,
    m.customer_pet_type,
    m.customer_pet_name,
    m.customer_pet_breed
FROM mock_data m
JOIN dim_customers c ON 
    m.customer_first_name = c.first_name AND 
    m.customer_last_name = c.last_name AND 
    m.customer_email = c.email AND 
    m.customer_age = c.age AND 
    m.customer_country = c.country
ON CONFLICT (customer_id, pet_name, pet_breed) DO NOTHING;


-- Заполнение измерения продавцов
INSERT INTO dim_sellers (first_name, last_name, email, country, postal_code)
SELECT DISTINCT 
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
FROM mock_data
ON CONFLICT (first_name, last_name, email, country) DO NOTHING;

-- Заполнение измерения продуктов
INSERT INTO dim_products (name, category, price, weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date, pet_category)
SELECT DISTINCT 
    product_name,
    product_category,
    product_price,
    product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    product_rating,
    product_reviews,
    product_release_date,
    product_expiry_date, 
    pet_category
FROM mock_data
ON CONFLICT (name, category, price, weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date) DO NOTHING;


-- Заполнение измерения магазинов
INSERT INTO dim_stores (name, location, city, state, country, phone, email)
SELECT 
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
FROM (
    SELECT DISTINCT 
        store_name,
        store_location,
        store_city,
        store_state,
        store_country,
        store_phone,
        store_email
    FROM mock_data
) AS unique_stores
ON CONFLICT (name, location, city, country) DO NOTHING;


-- Заполнение измерения поставщиков
INSERT INTO dim_suppliers (name, contact, email, phone, address, city, country)
SELECT 
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM (
    SELECT DISTINCT 
        supplier_name,
        supplier_contact,
        supplier_email,
        supplier_phone,
        supplier_address,
        supplier_city,
        supplier_country
    FROM mock_data
) AS unique_suppliers
ON CONFLICT (name, contact, email) DO NOTHING;



-- Заполнение таблицы фактов
INSERT INTO fact_sales (customer_id, seller_id, product_id, store_id, supplier_id, sale_date, quantity, total_price, unit_price)
SELECT 
    c.customer_id,
    s.seller_id,
    p.product_id,
    st.store_id,
    sup.supplier_id,
    m.sale_date,
    m.sale_quantity,
    m.sale_total_price,
    m.sale_total_price / NULLIF(m.sale_quantity, 0) as unit_price
FROM mock_data m
JOIN dim_customers c ON 
    m.customer_first_name = c.first_name AND 
    m.customer_last_name = c.last_name AND 
    m.customer_email = c.email AND 
    m.customer_age = c.age AND 
    m.customer_country = c.country
JOIN dim_sellers s ON 
    m.seller_first_name = s.first_name AND 
    m.seller_last_name = s.last_name AND 
    m.seller_email = s.email AND 
    m.seller_country = s.country
JOIN dim_products p ON 
    m.product_name = p.name AND 
    m.product_brand = p.brand AND 
    m.product_category = p.category AND 
    m.product_price = p.price AND
    m.product_weight = p.weight and
    m.product_material = p.material
JOIN dim_stores st ON 
    m.store_name = st.name AND 
    m.store_location = st.location AND 
    m.store_city = st.city AND 
    m.store_country = st.country
JOIN dim_suppliers sup ON 
    m.supplier_name = sup.name AND 
    m.supplier_contact = sup.contact AND 
    m.supplier_email = sup.email