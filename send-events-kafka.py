import config
import random
import time
import uuid
import json
import datetime
import kafka
from confluent_kafka import KafkaError


# Constants for account types and their weights
ACCOUNT_TYPES = ['checking', 'savings']
WEIGHTS_ACCOUNT_TYPES = [0.8, 0.2] # 80% chance for checking, 20% for savings

# Constant for currency codes per country
COUNTRY_CURRENCY_CODES = {'ES':'EUR', 'FR':'EUR', 'DE':'EUR', 'IT':'EUR', 'GB':'GBP', 'US':'USD'}

# Constant for the transaction type available
TRANSACTION_TYPES = ['income', 'expense', 'bizum', 'transfer']


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
clients_data = [
    {
        'client_id': str(uuid.uuid4()),
        'account_data': {
            'type': random.choices(ACCOUNT_TYPES, weights=WEIGHTS_ACCOUNT_TYPES)[0],
            'number': random.choice(list(COUNTRY_CURRENCY_CODES.keys())) + ''.join(str(random.randint(0, 9)) for _ in range(18))
        }
    } for i in range(1, 101)
]

event_sended = 1

# Generate random data for a transaction associated with a client and send to kafka topic
try:
    while True:

        kafka_producer = setup_kafka_producer()

        client = random.choice(clients_data)
        transaction_type = random.choice(TRANSACTION_TYPES)

        # If the destination of the event is a savings account, only transfers are allowed
        if client['account_data']['type'] == 'savings':
            transaction_type = 'transfer'

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
            'timestamp': ts_event, 
            'dataitem': str(dt_event),
            'client_id': client['client_id'],
            'account_number_client': client['account_data']['number'],
            'transaction_data': {
                'transaction_type': transaction_type,
                'amount': amount,
                'currency_code': COUNTRY_CURRENCY_CODES[client['account_data']['number'][:2]],
                'destination_account_number': random.choice(list(COUNTRY_CURRENCY_CODES.keys())) + ''.join(str(random.randint(0, 9)) for _ in range(18)),
                'account_type': client['account_data']['type'],
            }
        }

        send_to_kafka(config.TOPIC, event_transaction)
        print(f"Transaction #{event_sended} sent to the producer")
        event_sended += 1
        time.sleep(3)

except KeyboardInterrupt:
    kafka_producer.close()