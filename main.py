import webhdfsClient
import mqttClient

mqtt = mqttClient.getMqttClient()
mqtt.loop_forever()

hdfs = webhdfsClient.getClient('192.168.0.102', '50070', 'hduser')






