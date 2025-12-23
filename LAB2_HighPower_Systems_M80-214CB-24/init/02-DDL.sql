-- таблицы измерений
CREATE TABLE dim_customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INT,
    email VARCHAR(150),
    country VARCHAR(100),
    postal_code VARCHAR(20)
);

CREATE TABLE dim_pets (
    pet_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES dim_customers(customer_id),
    pet_type VARCHAR(50),
    pet_name VARCHAR(100),
    pet_breed VARCHAR(100),
    UNIQUE(customer_id, pet_name, pet_breed) 
);

CREATE TABLE dim_sellers (
    seller_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(150),
    country VARCHAR(100),
    postal_code VARCHAR(20),
    UNIQUE(first_name, last_name, email, country)
);

CREATE TABLE dim_products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(200),
    category VARCHAR(100),
    price DECIMAL(10,2),
    weight DECIMAL(10,2),
    color VARCHAR(50),
    size VARCHAR(50),
    brand VARCHAR(100),
    material VARCHAR(100),
    description TEXT,
    rating DECIMAL(3,1),
    reviews INT,
    release_date DATE,
    expiry_date DATE,
    pet_category VARCHAR(50),
    UNIQUE(name, category, price, weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date)
);


CREATE TABLE dim_stores (
    store_id SERIAL PRIMARY KEY,
    name VARCHAR(200),
    location VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(150),
    UNIQUE(name, location, city, country)
);

CREATE TABLE dim_suppliers (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(200),
    contact VARCHAR(100),
    email VARCHAR(150),
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    country VARCHAR(100),
    UNIQUE(name, contact, email)
);

-- таблица фактов
CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES dim_customers(customer_id),
    seller_id INT REFERENCES dim_sellers(seller_id),
    product_id INT REFERENCES dim_products(product_id),
    store_id INT REFERENCES dim_stores(store_id),
    supplier_id INT REFERENCES dim_suppliers(supplier_id),
    sale_date DATE,
    quantity INT,
    total_price DECIMAL(10,2),
    unit_price DECIMAL(10,2)
);


