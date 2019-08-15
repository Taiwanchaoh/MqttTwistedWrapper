"""
This code provides an example about how to use MqttTwisted_Wrapper with twisted server. 
"""
import paho.mqtt.client as mqtt
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
import socket
from MqttTwisted_Wrapper import MqttTwistedClient


# Define the topic
topic = 'test/test'


# Define on message callback
def on_message(client, userdata, msg):
    print(msg.topic + " " + msg.payload.decode())


# define connect callback
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe('test/#')


# initilize the paho client
client = MqttTwistedClient()
client.on_message = on_message
client.on_connect = on_connect

# connect to eclipse test broker
client.connect('test.mosquitto.org', 1883)


# keeping publish data every 2.5 seconds
def publish():
    client.publish(topic, b'hello', qos=1)


# create twisted looping call
task = LoopingCall(publish)
task.start(2.5).addErrback(lambda x: print(x))

# start the twisted server
try:
    reactor.run()
except InterruptedError:
    reactor.stop()
