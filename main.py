from mqtt_as import MQTTClient
from mqtt_local import config
import uasyncio as asyncio
import dht, machine, json
#librerias necesarias para sacar la mac
import network
import ubinascii

#establezco la id del dispositivo usando la mac del dispositivo
wlan = network.WLAN(network.STA_IF)
wlan.active(True)
#obtencion y decodificacion de la direc mac
ID_DEL_DISPOSITIVO= ubinascii.hexlify(wlan.config('mac'), ':').decode()
print('\nMAC(colocar esto en MQTTX):')
print(ID_DEL_DISPOSITIVO)

#Aca esta el sensor
d = dht.DHT11(machine.Pin(15)) 

#Para recibir los datos del MQTTX
async def messages(client):
    async for topic, msg, retained in client.queue:
        print(topic.decode(), msg.decode())

#este es para el funcionamiento del wifi cuando sube o baja
async def wifi_han(state):
    print('Wifi is ', 'up' if state else 'down')
    await asyncio.sleep(1)

# las cosas que me interesan recibir del broker
async def up(client):
    while True:
        await client.up.wait()
        client.up.clear()
        await client.subscribe(f'{ID_DEL_DISPOSITIVO}/setpoint',1)
        await client.subscribe(f'{ID_DEL_DISPOSITIVO}/periodo',1)
        await client.subscribe(f'{ID_DEL_DISPOSITIVO}/destello',1)
        await client.subscribe(f'{ID_DEL_DISPOSITIVO}/modo',1)
        await client.subscribe(f'{ID_DEL_DISPOSITIVO}/rele',1)
        await client.subscribe(f'{ID_DEL_DISPOSITIVO}/temperatura',1)
        await client.subscribe(f'{ID_DEL_DISPOSITIVO}/humedad',1)

async def main(client):
    await client.connect()
    for coroutine in (up, messages):
        asyncio.create_task(coroutine(client))
    n=0
    while True:
        await asyncio.sleep(5)
        try:
            d.measure()
            temperatura=d.temperature()
            humedad=d.humidity()
            datos= {
            "temperatura": temperatura,
            "humedad": humedad,
            "setpoint":25,
            "modo":"auto",
            "periodo": 5
            }
            conversion=json.dumps(datos)
            await client.publish(ID_DEL_DISPOSITIVO, conversion, qos = 1)
        except OSError as e:
            print("sin sensor")
        await asyncio.sleep(20)  # Broker is slow
        n +=1

# Define configuration
config["queue_len"]=1 
config['wifi_coro'] = wifi_han
config['ssl'] = True #para cifrar los datos

# Set up client
MQTTClient.DEBUG = True  # Optional
client = MQTTClient(config) #crea el objeto cliente
try:
    asyncio.run(main(client))
finally:
    client.close()
    asyncio.new_event_loop()
