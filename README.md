# Banking Events Simulator

## Description
This app is a Python-based simulator that generates simulated data for banking transactions, including income, expenses, mortgage payments, and transfers.

This dynamic financial information generator instantly sends the data to a Kafka topic.

## Running the app

Clone this repository to your local machine

Navigate to the project directory

#### Setting up a Virtual Environment

* **Conda (Mac OSX)**

```
conda create -n banking-events python=3.10
conda activate banking-events
```

Install dependencies with pip

```
pip install kafka-python
pip install confluent_kafka
```

Run the app

```
python send-events-kafka.py
```