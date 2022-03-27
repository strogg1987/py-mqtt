from multiprocessing.connection import wait
import random

import keyboard
import time
from paho.mqtt import client as mqtt_client

broker = 'localhost'
port = 1883
topic = "python/mqtt"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'
# username = 'emqx'
# password = 'public'


def connect(broker, port, client_id):
    def connect_mqtt():
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)
        client = mqtt_client.Client(client_id)
#    client.username_pw_set(username, password)
        client.on_connect = on_connect
        client.connect(broker, port)
        return client
    return connect_mqtt

connect_mqtt = connect(broker, port, client_id)


def pub_func(topic):
    def publish(client):
        msg = "test"+str(random.randint(0, 1000))
        result = client.publish(topic, msg)
    # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")
    return publish

publish = pub_func(topic)


def sub_func(topic):
    def subscribe(client: mqtt_client):
        def on_message(client, userdata, msg):
            print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        client.subscribe(topic+"2")
        client.on_message = on_message
    return subscribe

subscribe = sub_func(topic)


def new_func(connect_mqtt, publish, subscribe):
    def run():
        client = connect_mqtt()
        subscribe(client)
        client.loop_start()
        while True:
            keyboard.wait('ctrl')
            while keyboard.is_pressed('ctrl'):
                time.sleep(0.5)
            publish(client)
    return run

run = new_func(connect_mqtt, publish, subscribe)


if __name__ == '__main__':
    run()
