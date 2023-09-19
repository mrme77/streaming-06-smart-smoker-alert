"""
Author: Pasquale Salomone
Date: September 18, 2023

This script monitors temperature readings for a smoker and triggers a 'Smoker Alert' based on specific conditions. 
It listens to a RabbitMQ queue for incoming temperature messages, processes the data, and checks for temperature 
changes over a time window.

RabbitMQ configuration:
- Host: localhost
- Port: 5672 (Default RabbitMQ port)
- Queue: '01-smoker'

Alert Conditions:
- Temperature change threshold: 15°F
- Time window: 2.5 minutes (5 readings)
- Clear the deque every 5 readings

Alert Trigger:
- If the temperature decreases by 15°F or more within the specified time window, a 'Smoker Alert' is triggered.

Usage:
- Run this script to start monitoring smoker temperature readings. It will continuously listen for messages and 
  generate alerts when necessary.

To exit the script, press Ctrl+C.
"""


import pika
import time
from collections import deque

# RabbitMQ configuration
rabbit_host = 'localhost'
rabbit_port = 5672  
smoker_queue = '01-smoker'

# Constants for smoker alert conditions
SMOKER_TIME_WINDOW = 2.5  # minutes
SMOKER_DEQUE_MAX_LENGTH = int(SMOKER_TIME_WINDOW * 2)  # Assuming one reading every 0.5 minutes
SMOKER_TEMP_CHANGE_THRESHOLD = 15  # degrees F

# Create a deque to store temperature readings for the smoker
smoker_temperature_deque = deque(maxlen=SMOKER_DEQUE_MAX_LENGTH)

def show_smoker_alert(timestamp):
    #timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"Smoker Alert at: {timestamp}")

def smoker_callback(ch, method, properties, body):
    try:
        # Decode the message from bytes to a string
        body_str = body.decode('utf-8')
        timestamp = body_str.split()[1]
        temperature = float(body_str.split(':')[3])

        # Add the temperature reading to the deque
        smoker_temperature_deque.append(temperature)

        print(f"Received smoker temperature: {temperature}°F")
    except ValueError:
        print("Invalid temperature value in message body.")
    except Exception as e:
        print(f"Error processing message: {str(e)}")

    # Check if the deque has at least 5 readings
    if len(smoker_temperature_deque) >= 5:
        # Initialize a flag to track if the temperature change threshold is met
        threshold_met = False

        # Calculate the temperature change over the last 5 readings
        temp_changes = [smoker_temperature_deque[i] - smoker_temperature_deque[i - 1] for i in range(-1, -5, -1)]

        # Check if any of the temperature changes exceed the threshold
        if any(temp_change <= -SMOKER_TEMP_CHANGE_THRESHOLD for temp_change in temp_changes):
            threshold_met = True

        # If the threshold is met, trigger the alert
        if threshold_met:
            show_smoker_alert(timestamp)

        # Clear the deque every 5 readings
        if len(smoker_temperature_deque) % 5 == 0:
            smoker_temperature_deque.clear()


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host, port=rabbit_port))
    channel = connection.channel()

    channel.queue_declare(queue=smoker_queue, durable=True)

    channel.basic_consume(queue=smoker_queue, on_message_callback=smoker_callback, auto_ack=True)

    print("Smoker Consumer is waiting for messages. To exit, press Ctrl+C")
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nExiting peacefully...")
