from azure.eventhub import EventHubProducerClient, EventData
import json

# Replace with your Event Hubs namespace connection string
CONNECTION_STR = "Endpoint=sb://yrtk-analytics-prod.servicebus.windows.net/;SharedAccessKeyName=yrtk-click-analytics-prod-send;SharedAccessKey=lLxhnWhewSgMp+z3RWObZ+xhMwTFLX3Y6+AEhLAw5qc=;EntityPath=yrtk-click-analytics-prod"
EVENT_HUB_NAME = "yrtk-click-analytics-prod"

def send_event():
    # Create a producer client
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME
    )

    # Example JSON message
    message = {
        "id": 1,
        "event": "test_event",
        "description": "Hello from kunal!"
    }

    # Convert to JSON string
    event_data = EventData(json.dumps(message))

    # Send event
    with producer:
        event_batch = producer.create_batch()
        event_batch.add(event_data)
        producer.send_batch(event_batch)
        print("✅ Event sent successfully!")

if __name__ == "__main__":
    send_event()
