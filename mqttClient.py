import paho.mqtt.client as mqtt
import asyncio
from concurrent.futures import ThreadPoolExecutor
import time

_executor = ThreadPoolExecutor(3)


async def in_thread(func, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, func, *args)


def onConnect(client, userdata, flags, rc):
    print('Connected with result code ' + str(rc))
    client.subscribe('v2/#')


def onMessage(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))
    asyncio.run(in_thread(writePayload, msg.payload.decode('utf-8')))


def writePayload(payload):
    filename = time.strftime('%Y%m%d', time.localtime(time.time()))
    file = open('data/' + filename + '.packet', 'a+')
    file.write(str(payload) + '\n')


def getMqttClient():
    client = mqtt.Client()
    client.on_connect = onConnect
    client.on_message = onMessage
    client.username_pw_set('monitor', 'ahslxj1!')
    client.connect('59.16.212.210', 1883, 60)
    return client

