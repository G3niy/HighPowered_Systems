package com.lab3;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class StarSchemaJob {
    
    private static String safeGetString(Map<String, Object> record, String key) {
        Object value = record.get(key);
        if (value == null) return null;
        return value.toString();
    }
    
    private static Integer safeGetInt(Map<String, Object> record, String key) {
        Object value = record.get(key);
        if (value == null) return null;
        try {
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            return Integer.parseInt(value.toString());
        } catch (Exception e) {
            return null;
        }
    }
    
    private static Double safeGetDouble(Map<String, Object> record, String key) {
        Object value = record.get(key);
        if (value == null) return null;
        try {
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
            return Double.parseDouble(value.toString());
        } catch (Exception e) {
            return null;
        }
    }
    
    private static LocalDate safeParseDate(String dateStr, DateTimeFormatter formatter) {
        if (dateStr == null || dateStr.isEmpty()) return null;
        try {
            return LocalDate.parse(dateStr, formatter);
        } catch (Exception e) {
            return null;
        }
    }
    
    public static void main(String[] args) throws Exception {
        // Создание окружения Flink
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Настройка Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("petstore-transactions")
            .setGroupId("flink-star-schema-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        
        // Чтение данных из Kafka
        DataStream<String> jsonStream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );
        
        // Парсинг JSON и преобразование в модель звезда
        DataStream<Map<String, Object>> parsedStream = jsonStream
            .process(new ProcessFunction<String, Map<String, Object>>() {
                @Override
                public void processElement(
                    String value, 
                    ProcessFunction<String, Map<String, Object>>.Context ctx, 
                    Collector<Map<String, Object>> out
                ) throws Exception {
                    try {
                        JSONObject json = new JSONObject(value);
                        Map<String, Object> record = new HashMap<>();
                        
                        // Извлечение всех полей
                        json.keySet().forEach(key -> {
                            record.put(key, json.get(key));
                        });
                        
                        out.collect(record);
                    } catch (Exception e) {
                        System.err.println("Ошибка парсинга JSON: " + e.getMessage());
                        System.err.println("Проблемная строка: " + value);
                    }
                }
            });
        
        // JDBC Connection Options (общие для всех sinks)
        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl("jdbc:postgresql://postgres-lab3:5432/petstore")
            .withDriverName("org.postgresql.Driver")
            .withUsername("admin")
            .withPassword("password")
            .build();
        
        // JDBC Execution Options (общие для всех sinks)
        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
            .withBatchSize(1000)
            .withBatchIntervalMs(200)
            .withMaxRetries(5)
            .build();
        
        // Сохранение в таблицу dim_customer
        parsedStream.addSink(
            JdbcSink.sink(
                "INSERT INTO dim_customer (customer_first_name, customer_last_name, customer_age, " +
                "customer_email, customer_country, customer_postal_code, customer_pet_type, " +
                "customer_pet_name, customer_pet_breed) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (customer_email) DO UPDATE SET " +
                "customer_first_name = EXCLUDED.customer_first_name, " +
                "customer_last_name = EXCLUDED.customer_last_name, " +
                "customer_age = EXCLUDED.customer_age, " +
                "customer_country = EXCLUDED.customer_country, " +
                "customer_postal_code = EXCLUDED.customer_postal_code, " +
                "customer_pet_type = EXCLUDED.customer_pet_type, " +
                "customer_pet_name = EXCLUDED.customer_pet_name, " +
                "customer_pet_breed = EXCLUDED.customer_pet_breed",
                (statement, record) -> {
                    statement.setString(1, safeGetString(record, "customer_first_name"));
                    statement.setString(2, safeGetString(record, "customer_last_name"));
                    Integer age = safeGetInt(record, "customer_age");
                    if (age != null) {
                        statement.setInt(3, age);
                    } else {
                        statement.setNull(3, java.sql.Types.INTEGER);
                    }
                    statement.setString(4, safeGetString(record, "customer_email"));
                    statement.setString(5, safeGetString(record, "customer_country"));
                    statement.setString(6, safeGetString(record, "customer_postal_code"));
                    statement.setString(7, safeGetString(record, "customer_pet_type"));
                    statement.setString(8, safeGetString(record, "customer_pet_name"));
                    statement.setString(9, safeGetString(record, "customer_pet_breed"));
                },
                executionOptions,
                jdbcOptions
            )
        ).name("Save to dim_customer");
        
        // Сохранение в таблицу dim_seller
        parsedStream.addSink(
            JdbcSink.sink(
                "INSERT INTO dim_seller (seller_first_name, seller_last_name, seller_email, " +
                "seller_country, seller_postal_code) " +
                "VALUES (?, ?, ?, ?, ?) " +
                "ON CONFLICT (seller_email) DO UPDATE SET " +
                "seller_first_name = EXCLUDED.seller_first_name, " +
                "seller_last_name = EXCLUDED.seller_last_name, " +
                "seller_country = EXCLUDED.seller_country, " +
                "seller_postal_code = EXCLUDED.seller_postal_code",
                (statement, record) -> {
                    statement.setString(1, safeGetString(record, "seller_first_name"));
                    statement.setString(2, safeGetString(record, "seller_last_name"));
                    statement.setString(3, safeGetString(record, "seller_email"));
                    statement.setString(4, safeGetString(record, "seller_country"));
                    statement.setString(5, safeGetString(record, "seller_postal_code"));
                },
                executionOptions,
                jdbcOptions
            )
        ).name("Save to dim_seller");
        
        // Сохранение в таблицу dim_product
        parsedStream.addSink(
            JdbcSink.sink(
                "INSERT INTO dim_product (product_name, product_category, product_price, " +
                "product_quantity, product_weight, product_color, product_size, product_brand, " +
                "product_material, product_description, product_rating, product_reviews, " +
                "product_release_date, product_expiry_date, supplier_name, supplier_contact, " +
                "supplier_email, supplier_phone, supplier_address, supplier_city, " +
                "supplier_country, pet_category) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (product_name, product_brand) DO UPDATE SET " +
                "product_category = EXCLUDED.product_category, " +
                "product_price = EXCLUDED.product_price, " +
                "product_quantity = EXCLUDED.product_quantity, " +
                "product_weight = EXCLUDED.product_weight, " +
                "product_color = EXCLUDED.product_color, " +
                "product_size = EXCLUDED.product_size, " +
                "product_material = EXCLUDED.product_material, " +
                "product_description = EXCLUDED.product_description, " +
                "product_rating = EXCLUDED.product_rating, " +
                "product_reviews = EXCLUDED.product_reviews, " +
                "product_release_date = EXCLUDED.product_release_date, " +
                "product_expiry_date = EXCLUDED.product_expiry_date, " +
                "pet_category = EXCLUDED.pet_category",
                (statement, record) -> {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy");
                    
                    statement.setString(1, safeGetString(record, "product_name"));
                    statement.setString(2, safeGetString(record, "product_category"));
                    
                    Double price = safeGetDouble(record, "product_price");
                    if (price != null) {
                        statement.setDouble(3, price);
                    } else {
                        statement.setNull(3, java.sql.Types.DECIMAL);
                    }
                    
                    Integer quantity = safeGetInt(record, "product_quantity");
                    if (quantity != null) {
                        statement.setInt(4, quantity);
                    } else {
                        statement.setNull(4, java.sql.Types.INTEGER);
                    }
                    
                    Double weight = safeGetDouble(record, "product_weight");
                    if (weight != null) {
                        statement.setDouble(5, weight);
                    } else {
                        statement.setNull(5, java.sql.Types.DECIMAL);
                    }
                    
                    statement.setString(6, safeGetString(record, "product_color"));
                    statement.setString(7, safeGetString(record, "product_size"));
                    statement.setString(8, safeGetString(record, "product_brand"));
                    statement.setString(9, safeGetString(record, "product_material"));
                    statement.setString(10, safeGetString(record, "product_description"));
                    
                    Double rating = safeGetDouble(record, "product_rating");
                    if (rating != null) {
                        statement.setDouble(11, rating);
                    } else {
                        statement.setNull(11, java.sql.Types.DECIMAL);
                    }
                    
                    Integer reviews = safeGetInt(record, "product_reviews");
                    if (reviews != null) {
                        statement.setInt(12, reviews);
                    } else {
                        statement.setNull(12, java.sql.Types.INTEGER);
                    }
                    
                    LocalDate releaseDate = safeParseDate(safeGetString(record, "product_release_date"), formatter);
                    if (releaseDate != null) {
                        statement.setDate(13, Date.valueOf(releaseDate));
                    } else {
                        statement.setNull(13, java.sql.Types.DATE);
                    }
                    
                    LocalDate expiryDate = safeParseDate(safeGetString(record, "product_expiry_date"), formatter);
                    if (expiryDate != null) {
                        statement.setDate(14, Date.valueOf(expiryDate));
                    } else {
                        statement.setNull(14, java.sql.Types.DATE);
                    }
                    
                    statement.setString(15, safeGetString(record, "supplier_name"));
                    statement.setString(16, safeGetString(record, "supplier_contact"));
                    statement.setString(17, safeGetString(record, "supplier_email"));
                    statement.setString(18, safeGetString(record, "supplier_phone"));
                    statement.setString(19, safeGetString(record, "supplier_address"));
                    statement.setString(20, safeGetString(record, "supplier_city"));
                    statement.setString(21, safeGetString(record, "supplier_country"));
                    statement.setString(22, safeGetString(record, "pet_category"));
                },
                executionOptions,
                jdbcOptions
            )
        ).name("Save to dim_product");
        
        // Сохранение в таблицу dim_store
        parsedStream.addSink(
            JdbcSink.sink(
                "INSERT INTO dim_store (store_name, store_location, store_city, " +
                "store_state, store_country, store_phone, store_email) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (store_name, store_location) DO UPDATE SET " +
                "store_city = EXCLUDED.store_city, " +
                "store_state = EXCLUDED.store_state, " +
                "store_country = EXCLUDED.store_country, " +
                "store_phone = EXCLUDED.store_phone, " +
                "store_email = EXCLUDED.store_email",
                (statement, record) -> {
                    statement.setString(1, safeGetString(record, "store_name"));
                    statement.setString(2, safeGetString(record, "store_location"));
                    statement.setString(3, safeGetString(record, "store_city"));
                    statement.setString(4, safeGetString(record, "store_state"));
                    statement.setString(5, safeGetString(record, "store_country"));
                    statement.setString(6, safeGetString(record, "store_phone"));
                    statement.setString(7, safeGetString(record, "store_email"));
                },
                executionOptions,
                jdbcOptions
            )
        ).name("Save to dim_store");
        
        // Сохранение в таблицу dim_time
        parsedStream.addSink(
            JdbcSink.sink(
                "INSERT INTO dim_time (sale_date, sale_year, sale_month, sale_day, " +
                "sale_quarter, sale_day_of_week) " +
                "VALUES (?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (sale_date) DO NOTHING",
                (statement, record) -> {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy");
                    LocalDate saleDate = safeParseDate(safeGetString(record, "sale_date"), formatter);
                    
                    if (saleDate != null) {
                        statement.setDate(1, Date.valueOf(saleDate));
                        statement.setInt(2, saleDate.getYear());
                        statement.setInt(3, saleDate.getMonthValue());
                        statement.setInt(4, saleDate.getDayOfMonth());
                        statement.setInt(5, (saleDate.getMonthValue() - 1) / 3 + 1);
                        statement.setString(6, saleDate.getDayOfWeek().toString());
                    } else {
                        statement.setNull(1, java.sql.Types.DATE);
                        statement.setNull(2, java.sql.Types.INTEGER);
                        statement.setNull(3, java.sql.Types.INTEGER);
                        statement.setNull(4, java.sql.Types.INTEGER);
                        statement.setNull(5, java.sql.Types.INTEGER);
                        statement.setNull(6, java.sql.Types.VARCHAR);
                    }
                },
                executionOptions,
                jdbcOptions
            )
        ).name("Save to dim_time");
        
        // Сохранение в таблицу fact_sales
        // Для fact_sales нужно получить ID из dimension таблиц
        // Используем подзапросы для получения ID по уникальным полям
        parsedStream.addSink(
            JdbcSink.sink(
                "INSERT INTO fact_sales (customer_id, seller_id, product_id, store_id, time_id, " +
                "sale_quantity, sale_total_price) " +
                "SELECT " +
                "  (SELECT customer_id FROM dim_customer WHERE customer_email = ?), " +
                "  (SELECT seller_id FROM dim_seller WHERE seller_email = ?), " +
                "  (SELECT product_id FROM dim_product WHERE product_name = ? AND product_brand = ?), " +
                "  (SELECT store_id FROM dim_store WHERE store_name = ? AND store_location = ?), " +
                "  (SELECT time_id FROM dim_time WHERE sale_date = ?), " +
                "  ?, ? " +
                "WHERE EXISTS (SELECT 1 FROM dim_customer WHERE customer_email = ?) " +
                "  AND EXISTS (SELECT 1 FROM dim_seller WHERE seller_email = ?) " +
                "  AND EXISTS (SELECT 1 FROM dim_product WHERE product_name = ? AND product_brand = ?) " +
                "  AND EXISTS (SELECT 1 FROM dim_store WHERE store_name = ? AND store_location = ?) " +
                "  AND EXISTS (SELECT 1 FROM dim_time WHERE sale_date = ?)",
                (statement, record) -> {
                    String customerEmail = safeGetString(record, "customer_email");
                    String sellerEmail = safeGetString(record, "seller_email");
                    String productName = safeGetString(record, "product_name");
                    String productBrand = safeGetString(record, "product_brand");
                    String storeName = safeGetString(record, "store_name");
                    String storeLocation = safeGetString(record, "store_location");
                    
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy");
                    LocalDate saleDate = safeParseDate(safeGetString(record, "sale_date"), formatter);
                    
                    Integer saleQuantity = safeGetInt(record, "sale_quantity");
                    Double saleTotalPrice = safeGetDouble(record, "sale_total_price");
                    
                    // Параметры для SELECT подзапросов
                    statement.setString(1, customerEmail);
                    statement.setString(2, sellerEmail);
                    statement.setString(3, productName);
                    statement.setString(4, productBrand);
                    statement.setString(5, storeName);
                    statement.setString(6, storeLocation);
                    if (saleDate != null) {
                        statement.setDate(7, Date.valueOf(saleDate));
                    } else {
                        statement.setNull(7, java.sql.Types.DATE);
                    }
                    
                    if (saleQuantity != null) {
                        statement.setInt(8, saleQuantity);
                    } else {
                        statement.setNull(8, java.sql.Types.INTEGER);
                    }
                    
                    if (saleTotalPrice != null) {
                        statement.setDouble(9, saleTotalPrice);
                    } else {
                        statement.setNull(9, java.sql.Types.DECIMAL);
                    }
                    
                    // Параметры для WHERE EXISTS
                    statement.setString(10, customerEmail);
                    statement.setString(11, sellerEmail);
                    statement.setString(12, productName);
                    statement.setString(13, productBrand);
                    statement.setString(14, storeName);
                    statement.setString(15, storeLocation);
                    if (saleDate != null) {
                        statement.setDate(16, Date.valueOf(saleDate));
                    } else {
                        statement.setNull(16, java.sql.Types.DATE);
                    }
                },
                executionOptions,
                jdbcOptions
            )
        ).name("Save to fact_sales");
        
        // Запуск Flink job
        System.out.println("Запуск Flink Star Schema Job...");
        env.execute("Star Schema Transformation Job");
    }
}
