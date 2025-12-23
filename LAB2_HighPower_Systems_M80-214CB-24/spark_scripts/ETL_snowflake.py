from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

spark = (
    SparkSession.builder.appName("PostgreSQL")
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.8.jar")
    .getOrCreate()
)


db_properties = {
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://db:5432/db",
    "user": "root",
    "password": "root",
}

# Исходные данные
mock_data_df = (
    spark.read.format("jdbc")
    .option("driver", db_properties["driver"])
    .option("url", db_properties["url"])
    .option("dbtable", "mock_data")
    .option("user", db_properties["user"])
    .option("password", db_properties["password"])
    .load()
)

# Измерения клиентов
dim_customers_df = mock_data_df.select(
    col("customer_first_name").alias("first_name"),
    col("customer_last_name").alias("last_name"),
    col("customer_age").alias("age"),
    col("customer_email").alias("email"),
    col("customer_country").alias("country"),
    col("customer_postal_code").alias("postal_code"),
).distinct()

# Запись в PostgreSQL
dim_customers_df.write.format("jdbc").option("driver", db_properties["driver"]).option(
    "url", db_properties["url"]
).option("dbtable", "dim_customers").option("user", db_properties["user"]).option(
    "password", db_properties["password"]
).mode(
    "append"
).save()

dim_customers_with_id = (
    spark.read.format("jdbc")
    .option("driver", db_properties["driver"])
    .option("url", db_properties["url"])
    .option("dbtable", "dim_customers")
    .option("user", db_properties["user"])
    .option("password", db_properties["password"])
    .load()
)


# Измерения питомцев
dim_pets_temp = mock_data_df.select(
    col("customer_first_name").alias("first_name"),
    col("customer_last_name").alias("last_name"),
    col("customer_email").alias("email"),
    col("customer_age").alias("age"),
    col("customer_country").alias("country"),
    col("customer_pet_type").alias("pet_type"),
    col("customer_pet_name").alias("pet_name"),
    col("customer_pet_breed").alias("pet_breed"),
).distinct()

# Присоединение customer_id
dim_pets_df = (
    dim_pets_temp.join(
        dim_customers_with_id,
        ["first_name", "last_name", "email", "age", "country"],
        "inner",
    )
    .select(col("customer_id"), col("pet_type"), col("pet_name"), col("pet_breed"))
    .distinct()
)

# Запись в PostgreSQL
dim_pets_df.write.format("jdbc").option("driver", db_properties["driver"]).option(
    "url", db_properties["url"]
).option("dbtable", "dim_pets").option("user", db_properties["user"]).option(
    "password", db_properties["password"]
).mode(
    "append"
).save()

# Измерения продавцов
dim_sellers_df = mock_data_df.select(
    col("seller_first_name").alias("first_name"),
    col("seller_last_name").alias("last_name"),
    col("seller_email").alias("email"),
    col("seller_country").alias("country"),
    col("seller_postal_code").alias("postal_code"),
).distinct()


# Запись в PostgreSQL
dim_sellers_df.write.format("jdbc").option("driver", db_properties["driver"]).option(
    "url", db_properties["url"]
).option("dbtable", "dim_sellers").option("user", db_properties["user"]).option(
    "password", db_properties["password"]
).mode(
    "append"
).save()

# Измерения продуктов
dim_products_df = mock_data_df.select(
    col("product_name").alias("name"),
    col("product_category").alias("category"),
    col("product_price").alias("price"),
    col("product_weight").alias("weight"),
    col("product_color").alias("color"),
    col("product_size").alias("size"),
    col("product_brand").alias("brand"),
    col("product_material").alias("material"),
    col("product_description").alias("description"),
    col("product_rating").alias("rating"),
    col("product_reviews").alias("reviews"),
    col("product_release_date").alias("release_date"),
    col("product_expiry_date").alias("expiry_date"),
    col("pet_category").alias("pet_category"),
).distinct()


# Запись в PostgreSQL
dim_products_df.write.format("jdbc").option("driver", db_properties["driver"]).option(
    "url", db_properties["url"]
).option("dbtable", "dim_products").option("user", db_properties["user"]).option(
    "password", db_properties["password"]
).mode(
    "append"
).save()

# Заполнение измерения магазинов
dim_stores_df = mock_data_df.select(
    col("store_name").alias("name"),
    col("store_location").alias("location"),
    col("store_city").alias("city"),
    col("store_state").alias("state"),
    col("store_country").alias("country"),
    col("store_phone").alias("phone"),
    col("store_email").alias("email"),
).distinct()


# Запись в PostgreSQL
dim_stores_df.write.format("jdbc").option("driver", db_properties["driver"]).option(
    "url", db_properties["url"]
).option("dbtable", "dim_stores").option("user", db_properties["user"]).option(
    "password", db_properties["password"]
).mode(
    "append"
).save()

# Измерения поставщиков
dim_suppliers_df = mock_data_df.select(
    col("supplier_name").alias("name"),
    col("supplier_contact").alias("contact"),
    col("supplier_email").alias("email"),
    col("supplier_phone").alias("phone"),
    col("supplier_address").alias("address"),
    col("supplier_city").alias("city"),
    col("supplier_country").alias("country"),
).distinct()


# Запись в PostgreSQL
dim_suppliers_df.write.format("jdbc").option("driver", db_properties["driver"]).option(
    "url", db_properties["url"]
).option("dbtable", "dim_suppliers").option("user", db_properties["user"]).option(
    "password", db_properties["password"]
).mode(
    "append"
).save()

dim_customers_df = (
    spark.read.format("jdbc")
    .option("driver", db_properties["driver"])
    .option("url", db_properties["url"])
    .option("dbtable", "dim_customers")
    .option("user", db_properties["user"])
    .option("password", db_properties["password"])
    .load()
)

dim_sellers_df = (
    spark.read.format("jdbc")
    .option("driver", db_properties["driver"])
    .option("url", db_properties["url"])
    .option("dbtable", "dim_sellers")
    .option("user", db_properties["user"])
    .option("password", db_properties["password"])
    .load()
)

dim_products_df = (
    spark.read.format("jdbc")
    .option("driver", db_properties["driver"])
    .option("url", db_properties["url"])
    .option("dbtable", "dim_products")
    .option("user", db_properties["user"])
    .option("password", db_properties["password"])
    .load()
)

dim_stores_df = (
    spark.read.format("jdbc")
    .option("driver", db_properties["driver"])
    .option("url", db_properties["url"])
    .option("dbtable", "dim_stores")
    .option("user", db_properties["user"])
    .option("password", db_properties["password"])
    .load()
)


dim_suppliers_df = (
    spark.read.format("jdbc")
    .option("driver", db_properties["driver"])
    .option("url", db_properties["url"])
    .option("dbtable", "dim_suppliers")
    .option("user", db_properties["user"])
    .option("password", db_properties["password"])
    .load()
)

# Заполнение таблицы фактов
fact_sales_df = (
    mock_data_df.join(
        dim_customers_df,
        (mock_data_df.customer_first_name == dim_customers_df.first_name)
        & (mock_data_df.customer_last_name == dim_customers_df.last_name)
        & (mock_data_df.customer_email == dim_customers_df.email)
        & (mock_data_df.customer_age == dim_customers_df.age)
        & (mock_data_df.customer_country == dim_customers_df.country),
        "inner",
    )
    .join(
        dim_sellers_df,
        (mock_data_df.seller_first_name == dim_sellers_df.first_name)
        & (mock_data_df.seller_last_name == dim_sellers_df.last_name)
        & (mock_data_df.seller_email == dim_sellers_df.email)
        & (mock_data_df.seller_country == dim_sellers_df.country),
        "inner",
    )
    .join(
        dim_products_df,
        (mock_data_df.product_name == dim_products_df.name)
        & (mock_data_df.product_brand == dim_products_df.brand)
        & (mock_data_df.product_category == dim_products_df.category)
        & (mock_data_df.product_price == dim_products_df.price)
        & (mock_data_df.product_weight == dim_products_df.weight)
        & (mock_data_df.product_material == dim_products_df.material),
        "inner",
    )
    .join(
        dim_stores_df,
        (mock_data_df.store_name == dim_stores_df.name)
        & (mock_data_df.store_location == dim_stores_df.location)
        & (mock_data_df.store_city == dim_stores_df.city)
        & (mock_data_df.store_country == dim_stores_df.country),
        "inner",
    )
    .join(
        dim_suppliers_df,
        (mock_data_df.supplier_name == dim_suppliers_df.name)
        & (mock_data_df.supplier_contact == dim_suppliers_df.contact)
        & (mock_data_df.supplier_email == dim_suppliers_df.email),
        "inner",
    )
    .select(
        col("customer_id"),
        col("seller_id"),
        col("product_id"),
        col("store_id"),
        col("supplier_id"),
        col("sale_date"),
        col("sale_quantity").alias("quantity"),
        col("sale_total_price").alias("total_price"),
        (col("sale_total_price") / col("sale_quantity")).alias("unit_price"),
    )
)


# Запись в PostgreSQL
fact_sales_df.write.format("jdbc").option("driver", db_properties["driver"]).option(
    "url", db_properties["url"]
).option("dbtable", "fact_sales").option("user", db_properties["user"]).option(
    "password", db_properties["password"]
).mode(
    "append"
).save()

# Закрытие Spark сессии
spark.stop()
print("Session done")
