from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


def create_spark_session():
    return (
        SparkSession.builder.appName("DataMart")
        .config(
            "spark.jars",
            "/opt/spark/jars/clickhouse-jdbc-0.9.2-all-dependencies.jar,"
            "/opt/spark/jars/postgresql-42.7.8.jar,"
            "/opt/spark/jars/spark-cassandra-connector_2.13-3.5.1.jar,",
        )
        .config("spark.cassandra.connection.host", "cassandra")  # ← ДОБАВЬТЕ ЭТО
        .config("spark.cassandra.connection.port", "9042")       # ← И ЭТО
        .getOrCreate()
    )


def read_postgres_tables(spark):
    """Чтение таблиц из Postre"""

    pg_properties = {
        "driver": "org.postgresql.Driver",
        "user": "root",
        "password": "root",
    }
    pg_url = "jdbc:postgresql://db:5432/db"

    # таблицы измерений
    dim_customers = spark.read.jdbc(
        url=pg_url, table="dim_customers", properties=pg_properties
    )
    dim_products = spark.read.jdbc(
        url=pg_url, table="dim_products", properties=pg_properties
    )
    dim_stores = spark.read.jdbc(
        url=pg_url, table="dim_stores", properties=pg_properties
    )
    dim_suppliers = spark.read.jdbc(
        url=pg_url, table="dim_suppliers", properties=pg_properties
    )

    # таблицы фактов
    fact_sales = spark.read.jdbc(
        url=pg_url, table="fact_sales", properties=pg_properties
    )

    return dim_customers, dim_products, dim_stores, dim_suppliers, fact_sales


def create_product_sales_mart(dim_products, fact_sales):
    """Витрина продаж по продуктам"""

    product_sales = fact_sales.groupBy("product_id").agg(
        sum("quantity").alias("total_quantity"),
        sum("total_price").alias("total_revenue"),
        count("sale_id").alias("sales_count"),
    )

    product_mart = (
        product_sales.join(dim_products, "product_id", "left")
        .select(
            "product_id",
            "name",
            "category",
            "total_quantity",
            "total_revenue",
            "sales_count",
            "rating",
            "reviews",
        )
        .fillna({"rating": 0, "reviews": 0, "total_quantity": 0, "total_revenue": 0})
    )

    # Топ-10 самых продаваемых
    top_10_products = product_mart.orderBy(desc("total_quantity")).limit(10)

    # Общая выручка по категориям
    category_revenue = product_mart.groupBy("category").agg(
        sum("total_revenue").alias("category_revenue"),
        sum("total_quantity").alias("category_total_quantity"),
        count("product_id").alias("products_count"),
    )

    # Средний рейтинг и количество отзывов
    product_ratings_mart = dim_products.select(
        "product_id", "name", "category", "rating", "reviews"
    ).fillna({"rating": 0, "reviews": 0})

    return product_mart, top_10_products, category_revenue, product_ratings_mart


def create_customer_sales_mart(dim_customers, fact_sales):
    """Витрина продаж по клиентам"""

    customer_sales = fact_sales.groupBy("customer_id").agg(
        sum("total_price").alias("total_spent"),
        count("sale_id").alias("purchase_count"),
        avg("total_price").alias("avg_order_value"),
    )

    customer_mart = (
        customer_sales.join(dim_customers, "customer_id", "left")
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "country",
            "total_spent",
            "purchase_count",
            "avg_order_value",
        )
        .fillna({"country": "Unknown"})
    )

    # Топ-10
    top_10_customers = customer_mart.orderBy(desc("total_spent")).limit(10)

    # Распределение по странам
    country_distribution = (
        customer_mart.groupBy("country")
        .agg(
            count("customer_id").alias("customer_count"),
            sum("total_spent").alias("total_spent_by_country"),
        )
        .orderBy(desc("total_spent_by_country"))
    )

    return customer_mart, top_10_customers, country_distribution


def create_time_sales_mart(fact_sales):
    """Витрина продаж по времени"""

    time_mart = (
        fact_sales.filter(col("sale_date").isNotNull())
        .withColumn("year", year("sale_date"))
        .withColumn("month", month("sale_date"))
        .withColumn("year_month", date_format("sale_date", "yyyy-MM"))
    )

    # Месячные тренды
    monthly_trends = (
        time_mart.groupBy("year", "month", "year_month")
        .agg(
            sum("total_price").alias("monthly_revenue"),
            count("sale_id").alias("monthly_orders"),
            avg("total_price").alias("avg_order_size_monthly"),
        )
        .orderBy("year", "month")
    )

    # Годовые тренды
    yearly_trends = (
        time_mart.groupBy("year")
        .agg(
            sum("total_price").alias("yearly_revenue"),
            count("sale_id").alias("yearly_orders"),
            avg("total_price").alias("avg_order_size_yearly"),
        )
        .orderBy("year")
    )

    # Сравнение периодов
    period_comparison = monthly_trends.groupBy("month").agg(
        avg("monthly_revenue").alias("avg_monthly_revenue"),
        avg("monthly_orders").alias("avg_monthly_orders"),
    )

    return monthly_trends, yearly_trends, period_comparison


def create_store_sales_mart(dim_stores, fact_sales):
    """Витрина продаж по магазинам"""

    store_sales = fact_sales.groupBy("store_id").agg(
        sum("total_price").alias("total_revenue"),
        count_distinct("sale_id").alias("total_orders"),
        count_distinct("customer_id").alias("unique_customers"),
        sum("quantity").alias("total_quantity_sold"),
        avg("total_price").alias("avg_order_value"),
    )

    store_mart = (
        store_sales.join(dim_stores, "store_id", "left")
        .select(
            "store_id",
            "name",
            "city",
            "country",
            "total_revenue",
            "total_orders",
            "unique_customers",
            "total_quantity_sold",
            "avg_order_value",
        )
        .fillna({"country": "Unknown", "city": "Unknown"})
    )

    # Топ-5 по выручке
    top_5_stores = store_mart.orderBy(desc("total_revenue")).limit(5)

    # Распределение по городам и странам
    geo_distribution = (
        store_mart.groupBy("country", "city")
        .agg(
            sum("total_revenue").alias("total_revenue_by_location"),
            count("store_id").alias("store_count"),
            sum("total_orders").alias("total_orders_by_location"),
            avg("avg_order_value").alias("avg_order_value_by_location"),
        )
        .orderBy(desc("total_revenue_by_location"))
    )

    return store_mart, top_5_stores, geo_distribution


def create_supplier_sales_mart(dim_suppliers, fact_sales):
    """Витрина продаж по поставщикам"""

    sales_with_supplier = fact_sales.filter(col("supplier_id").isNotNull())

    supplier_sales = sales_with_supplier.groupBy("supplier_id").agg(
        sum("total_price").alias("total_revenue"),
        count_distinct("sale_id").alias("total_sales"),
        count_distinct("product_id").alias("unique_products_sold"),
        sum("quantity").alias("total_quantity_sold"),
        avg("unit_price").alias("avg_product_price"),
        min("unit_price").alias("min_product_price"),
        max("unit_price").alias("max_product_price"),
    )

    supplier_mart = (
        supplier_sales.join(dim_suppliers, "supplier_id", "left")
        .select(
            "supplier_id",
            "name",
            "country",
            "contact",
            "total_revenue",
            "total_sales",
            "unique_products_sold",
            "total_quantity_sold",
            "avg_product_price",
            "min_product_price",
            "max_product_price",
        )
        .fillna({"country": "Unknown"})
    )

    # Топ-5 поставщиков по выручке
    top_5_suppliers = supplier_mart.orderBy(desc("total_revenue")).limit(5)

    # Распределение по странам
    supplier_country_dist = (
        supplier_mart.groupBy("country")
        .agg(
            sum("total_revenue").alias("total_revenue_by_country"),
            count("supplier_id").alias("supplier_count"),
            sum("total_sales").alias("total_sales_by_country"),
            avg("avg_product_price").alias("avg_product_price_by_country"),
        )
        .orderBy(desc("total_revenue_by_country"))
    )

    return supplier_mart, top_5_suppliers, supplier_country_dist


def create_product_quality_mart(dim_products, fact_sales):
    """Витрина качества продукции"""

    # Статистика продаж по продуктам
    product_sales_stats = fact_sales.groupBy("product_id").agg(
        sum("quantity").alias("total_sold"),
        sum("total_price").alias("total_revenue"),
        count_distinct("sale_id").alias("sales_count"),
        avg("unit_price").alias("avg_sale_price"),
    )

    quality_mart = (
        product_sales_stats.join(dim_products, "product_id", "left")
        .select(
            "product_id",
            "name",
            "category",
            "rating",
            "reviews",
            "total_sold",
            "total_revenue",
            "sales_count",
            "avg_sale_price",
        )
        .fillna({"rating": 0, "reviews": 0, "total_sold": 0, "total_revenue": 0})
    )

    # Продукты с наивысшим рейтингом (минимум 10 отзывов)
    highest_rated = (
        quality_mart.filter(col("reviews") >= 10).orderBy(desc("rating")).limit(10)
    )

    # Продукты с наименьшим рейтингом (минимум 1 отзыв)
    lowest_rated = quality_mart.filter(col("reviews") >= 1).orderBy("rating").limit(10)

    # Продукты с наибольшим количеством отзывов
    most_reviewed = quality_mart.orderBy(desc("reviews")).limit(10)

    # Анализ корреляции между рейтингом и продажами
    correlation_analysis = quality_mart.filter(col("reviews") > 0).agg(
        corr("rating", "total_sold").alias("corr_rating_sales"),
        corr("rating", "total_revenue").alias("corr_rating_revenue"),
        corr("rating", "reviews").alias("corr_rating_reviews"),
    )

    return (
        quality_mart,
        highest_rated,
        lowest_rated,
        most_reviewed,
        correlation_analysis,
    )


def write_to_clickhouse(df, table_name):
    """Запись DataFrame в ClickHouse"""

    try:
        df.write.format("jdbc").option(
            "url", "jdbc:clickhouse://clickhouse:8123/clickhouse"
        ).option("dbtable", table_name).option("user", "admin").option(
            "password", "password"
        ).option(
            "driver", "com.clickhouse.jdbc.ClickHouseDriver"
        ).mode(
            "append"
        ).save()
        print(f"Успешно записано в ClickHouse: {table_name}")
    except Exception as e:
        print(e)


def write_to_cassandra(df, table_name):
    """Запись DataFrame в Cassandra"""
    try:
        df.write.format("org.apache.spark.sql.cassandra").options(
            table=table_name, keyspace="cassandra"
        ).mode("append").save()
        print(f"Успешно записано в Cassandra: {table_name}")
    except Exception as e:
        print(f"Ошибка при записи в Cassandra {table_name}: {str(e)}")


def write_to_all_databases(df, table_name):
    """Запись во все базы данных"""
    print(f"Начата загрузка {table_name} во все БД...")

    # ClickHouse
    write_to_clickhouse(df, table_name)

    # Cassandra
    write_to_cassandra(df, table_name)

    print(f"Завершена загрузка {table_name} во все БД")


spark = create_spark_session()

dim_customers, dim_products, dim_stores, dim_suppliers, fact_sales = (
    read_postgres_tables(spark)
)

# витрины

# 1. Витрина продаж по продуктам
product_mart, top_10_products, category_revenue, category_ratings = (
    create_product_sales_mart(dim_products, fact_sales)
)

write_to_all_databases(category_ratings, "product_ratings_mart")
write_to_all_databases(top_10_products, "top_10_products")
write_to_all_databases(category_revenue, "category_revenue_mart")
print("Витрина продаж по продуктам")

# 2. Витрина продаж по клиентам
customer_mart, top_10_customers, country_distribution = create_customer_sales_mart(
    dim_customers, fact_sales
)
write_to_all_databases(customer_mart, "customer_sales_mart")
write_to_all_databases(top_10_customers, "top_10_customers")
write_to_all_databases(country_distribution, "customer_country_distribution")
print("Витрина продаж по клиентам")

# 3. Витрина продаж по времени
monthly_trends, yearly_trends, period_comparison = create_time_sales_mart(fact_sales)
write_to_all_databases(monthly_trends, "monthly_trends_mart")
write_to_all_databases(yearly_trends, "yearly_trends_mart")
write_to_all_databases(period_comparison, "monthly_comparison_mart")
print("Витрина продаж по времени")

# 4. Витрина продаж по магазинам
store_mart, top_5_stores, geo_distribution = create_store_sales_mart(
    dim_stores, fact_sales
)
write_to_all_databases(store_mart, "store_sales_mart")
write_to_all_databases(top_5_stores, "top_5_stores")
write_to_all_databases(geo_distribution, "store_geo_distribution")
print("Витрина продаж по магазинам")

# 5. Витрина продаж по поставщикам
supplier_mart, top_5_suppliers, supplier_country_dist = create_supplier_sales_mart(
    dim_suppliers, fact_sales
)
write_to_all_databases(supplier_mart, "supplier_sales_mart")
write_to_all_databases(top_5_suppliers, "top_5_suppliers")
write_to_all_databases(supplier_country_dist, "supplier_country_distribution")
print("Витрина продаж по поставщикам")

# 6. Витрина качества продукции
quality_mart, highest_rated, lowest_rated, most_reviewed, correlation_analysis = (
    create_product_quality_mart(dim_products, fact_sales)
)
write_to_all_databases(quality_mart, "product_quality_mart")
write_to_all_databases(highest_rated, "highest_rated_products")
write_to_all_databases(lowest_rated, "lowest_rated_products")
write_to_all_databases(most_reviewed, "most_reviewed_products")
write_to_all_databases(correlation_analysis, "correlation_analysis_mart")
print("Витрина качества продукции")
