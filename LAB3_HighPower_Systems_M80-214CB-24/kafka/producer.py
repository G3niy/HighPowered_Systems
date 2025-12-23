import json
import csv
import time
from kafka import KafkaProducer
import os
import glob

class CSVToKafkaProducer:
    def __init__(self, kafka_broker='localhost:9092', topic='petstore-transactions'):
        self.kafka_broker = kafka_broker
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
    
    def csv_to_json(self, csv_file_path):
        # Чтение CSV файла и преобразование в JSON
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                # Преобразуем числовые поля
                numeric_fields = [
                    'id', 'customer_age', 'product_price', 'product_quantity',
                    'sale_quantity', 'sale_total_price', 'product_weight',
                    'product_rating', 'product_reviews'
                ]
                
                for field in numeric_fields:
                    if field in row and row[field]:
                        try:
                            if '.' in row[field]:
                                row[field] = float(row[field])
                            else:
                                row[field] = int(row[field])
                        except ValueError:
                            pass
                
                yield row
    
    def send_to_kafka(self, data_directory, delay=0.1):
        # Отправка всех CSV файлов из директории в Kafka
        # Ищем файлы с разными вариантами названий (mock_data, MOCK_DATA, с пробелами)
        csv_files = glob.glob(os.path.join(data_directory, "*MOCK_DATA*.csv"))
        csv_files.extend(glob.glob(os.path.join(data_directory, "*mock_data*.csv")))
        
        if not csv_files:
            print(f"CSV файлы не найдены в директории: {data_directory}")
            return
        
        total_records = 0
        
        for csv_file in sorted(csv_files):
            print(f"Обработка файла: {csv_file}")
            
            for record in self.csv_to_json(csv_file):
                try:
                    # Отправка сообщения в Kafka
                    self.producer.send(self.topic, value=record)
                    total_records += 1
                    
                    if total_records % 100 == 0:
                        print(f"Отправлено {total_records} записей...")
                    
                    time.sleep(delay)  
                    
                except Exception as e:
                    print(f"Ошибка при отправке записи: {e}")
            
            print(f"Файл {csv_file} обработан")
        
        # Ожидание отправки всех сообщений
        self.producer.flush()
        print(f"Всего отправлено {total_records} записей в топик {self.topic}")
    
    def close(self):
        # Закрытие соединения с Kafka
        self.producer.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='CSV to Kafka Producer')
    parser.add_argument('--data-dir', default=os.getenv('DATA_DIR', '/app/data'), help='Директория с CSV файлами')
    parser.add_argument('--kafka-broker', default=os.getenv('KAFKA_BROKER', 'localhost:9092'), help='Адрес Kafka брокера')
    parser.add_argument('--topic', default=os.getenv('KAFKA_TOPIC', 'petstore-transactions'), help='Название Kafka топика')
    parser.add_argument('--delay', type=float, default=0.01, help='Задержка между сообщениями')
    
    args = parser.parse_args()
    
    producer = CSVToKafkaProducer(
        kafka_broker=args.kafka_broker,
        topic=args.topic
    )
    
    try:
        producer.send_to_kafka(args.data_dir, delay=args.delay)
    except KeyboardInterrupt:
        print("\nПрервано пользователем")
    finally:
        producer.close()