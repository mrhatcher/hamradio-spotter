import paho.mqtt.client as mqtt

BROKER = "138.68.151.174"
PORT = 1883

def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected: {reason_code}")
    client.subscribe("#")
    print("Subscribed to # (all topics)")

def on_message(client, userdata, msg):
    print(f"\nTOPIC:   {msg.topic}")
    print(f"PAYLOAD: {msg.payload}")

def on_subscribe(client, userdata, mid, reason_codes, properties):
    print(f"Subscribe ack: {reason_codes}")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message
client.on_subscribe = on_subscribe

print(f"Connecting to {BROKER}:{PORT} ...")
client.connect(BROKER, PORT, 60)
client.loop_forever()
