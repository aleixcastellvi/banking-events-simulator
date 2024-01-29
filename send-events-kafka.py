import config
import random
import time
import uuid
import json
import datetime
import kafka
from confluent_kafka import KafkaError


# Constants
TRANSACTIONS_STATUS = ["Completed", "Cancelled"]
WEIGHTS_TRANSACTION = [0.9, 0.1] # 90% de probabilidad de transacciones completadas, 10% de transacciones canceladas

TRANSACTIONS_TYPES = ['Income', 'Expense', 'Bizum', 'Transfer']
DEVICES_TYPE = ["Smartphone", "Laptop", "Desktop", "Smartwatch"]
PAYMENTS_METHOD = ["Credit Card", "Debit Card", "Bank Transfer", "E-wallet", "PayPal"]

# Constant for currency codes per country
COUNTRY_CURRENCY_CODES = {
    'ES': {'currency': 'EUR', 'country_name': 'Spain'},
    'FR': {'currency': 'EUR', 'country_name': 'France'},
    'DE': {'currency': 'EUR', 'country_name': 'Germany'},
    'IT': {'currency': 'EUR', 'country_name': 'Italy'},
    'GB': {'currency': 'GBP', 'country_name': 'United Kingdom'},
    'US': {'currency': 'USD', 'country_name': 'United States'}
}

def setup_kafka_producer():
    """
    Set up the Kafka producer
    """
    return kafka.KafkaProducer(
        bootstrap_servers = config.KAFKA_URI, 
        api_version = '0.9', 
        value_serializer = lambda v: json.dumps(v).encode('utf-8'))

def send_to_kafka(topic, event_data):
    """
    Sends event data to the Kafka producer

    Params:
        topic (str): The Kafka topic to which the event should be sent
        event_data (dict): The event data to be sent
    """
    try:
        future = kafka_producer.send(topic, event_data)
        kafka_producer.flush()  # Don't save data in a buffer and send immediately
        return future
    
    except KafkaError as e:
        print(f"Error sending to Kafka: {e}")
        return None

# Generate data list for 100 clients
clients_data = [str(uuid.uuid4()) for _ in range(1, 101)]

event_sended = 1

# Generate random data for a transaction associated with a client and send to kafka topic
try:
    while True:

        kafka_producer = setup_kafka_producer()
        
        # Randomly generate transaction data
        client = random.choice(clients_data)

        country_code = random.choice(list(COUNTRY_CURRENCY_CODES.keys()))
        account_number_client = country_code + ''.join(str(random.randint(0, 9)) for _ in range(18))
        currency = COUNTRY_CURRENCY_CODES[country_code]['currency']
        country_name = COUNTRY_CURRENCY_CODES[country_code]['country_name']
        
        destination_country_code = random.choice(list(COUNTRY_CURRENCY_CODES.keys()))
        destination_account_number = destination_country_code + ''.join(str(random.randint(0, 9)) for _ in range(18))
        destination_currency = COUNTRY_CURRENCY_CODES[destination_country_code]['currency']
        destination_country_name = COUNTRY_CURRENCY_CODES[destination_country_code]['country_name']
        transaction_type = random.choice(TRANSACTIONS_TYPES)

        device_type = random.choice(DEVICES_TYPE)
        transaction_status = random.choices(TRANSACTIONS_STATUS, weights=WEIGHTS_TRANSACTION)[0]
        payment_method = random.choice(PAYMENTS_METHOD)

        if transaction_type == 'income':
            amount = round(random.uniform(1000, 5000), 2)
        elif transaction_type == 'expense':
            amount = round(random.uniform(10, 200), 2)
        elif transaction_type == 'bizum':
            amount = round(random.uniform(1, 100), 2)
        else:
            amount = round(random.uniform(50, 500), 2)

        ts_event = int(time.time() * 1000) # timestamp in milliseconds
        dt_event = datetime.datetime.fromtimestamp(ts_event/1000)

        event_transaction = {
            "txn_id": "txn-" + str(uuid.uuid4()),
            "timestamp": ts_event, 
            "dataitem": str(dt_event),
            "client_id": client,
            "account_number_client": account_number_client,
            "txn_data": {
                "destination_account_number": destination_account_number,
                "txn_type": transaction_type,
                "amount": amount,
                "currency_code": destination_currency,
                "destination_country": destination_country_name
            },
            "country": country_name,
            "device_type": device_type,
            "txn_status": transaction_status,
            "payment_method": payment_method,

        }

        send_to_kafka(config.TOPIC, event_transaction)
        #print(json.dumps(event_transaction, indent=2))
        print(f"Transaction #{event_sended} sent to the producer")
        event_sended += 1
        time.sleep(3)

except KeyboardInterrupt:
    kafka_producer.close()