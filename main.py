from machine import Pin
from mqtt_as import MQTTClient
from mqtt_as import config
import settings
import uasyncio as asyncio
import dht, machine, json
# Librerías necesarias para sacar la MAC
import network
import ubinascii

wlan = network.WLAN(network.STA_IF)
wlan.active(True)
ID_DEL_DISPOSITIVO = ubinascii.hexlify(wlan.config('mac'), ':').decode()
print('\nMAC(colocar esto en el bot de Telegram):')
print(ID_DEL_DISPOSITIVO)

# Para el destello
evento = asyncio.Event()

# Acá está el sensor
d = dht.DHT11(machine.Pin(15)) 
# El led de destello
led = machine.Pin("LED", Pin.OUT)
# Pin para el relé   
r = machine.Pin(16, Pin.OUT)
r.value(1) # Apagado inicial (Activo en bajo)


try:
    with open("estado.json", "r") as f:
        estado = json.load(f)
    print("datos almacenados cargados")
except OSError:
    print("sin datos guardados\nse guardan los de default")
    estado = {
        "setpoint": 25.0,
        "modo": "auto", 
        "periodo": 25,
        "rele_orden": False  
    }

# Guardado de datos en la Flash
def guardar_datos():  
    try:
        with open("estado.json", "w") as f:
            json.dump(estado, f)
    except OSError:
        print("Error guardando en flash")


async def messages(client):
    async for topic, msg, retained in client.queue:
        instruccion = topic.decode()
        valor = msg.decode()
        band = False

        print(f"comando recibido: {instruccion}->{valor}")

        if instruccion.endswith('/setpoint'):
            estado["setpoint"] = float(valor)
            band = True
        elif instruccion.endswith('/periodo'):
            estado["periodo"] = int(valor)
            band = True
        elif instruccion.endswith('/modo'):
            estado["modo"] = valor.lower() 
            band = True
        elif instruccion.endswith('/rele'):
            estado["rele_orden"] = (valor == "1")
            band = True
        elif instruccion.endswith('/destello'):
            evento.set() 
            band = True
        if band:
            guardar_datos()

# Tarea encargada del parpadeo físico del LED
async def destello():
    while True:
        await evento.wait()
        evento.clear()
        for _ in range(10):
            led.toggle()
            await asyncio.sleep_ms(200)
        led.off()

# estado del Wi-Fi
async def wifi_han(state):
    print('Wifi is ', 'up' if state else 'down')
    await asyncio.sleep(1)

# Suscripción a los tópicos del Bot
async def up(client):
    while True:
        await client.up.wait()
        client.up.clear()
        await client.subscribe(f'{ID_DEL_DISPOSITIVO}/setpoint', 1)
        await client.subscribe(f'{ID_DEL_DISPOSITIVO}/periodo', 1)
        await client.subscribe(f'{ID_DEL_DISPOSITIVO}/destello', 1)
        await client.subscribe(f'{ID_DEL_DISPOSITIVO}/modo', 1)
        await client.subscribe(f'{ID_DEL_DISPOSITIVO}/rele', 1)


async def main(client):
    await client.connect()
    asyncio.create_task(up(client))
    asyncio.create_task(messages(client))
    asyncio.create_task(destello())
    while True:
        try:
            d.measure()
            temperatura = d.temperature()
            humedad = d.humidity()

            if estado["modo"] == "auto":
                if temperatura > estado["setpoint"]:
                    r.value(0) # Enciende (Relé activo en bajo)
                else:
                    r.value(1) # Apaga
            else:
                if estado["rele_orden"]:
                    r.value(0)
                else:
                    r.value(1)
    
            datos = {
                "temperatura": temperatura,
                "humedad": humedad,
                "setpoint": estado["setpoint"],
                "periodo": estado["periodo"],
                "modo": estado["modo"]
            }
            conversion = json.dumps(datos)
            await client.publish(ID_DEL_DISPOSITIVO, conversion, qos=1)
        except OSError:
            print("sin sensor")
        await asyncio.sleep(estado["periodo"])  


# Mapeo de settings.py a la librería
config['ssid'] = settings.SSID
config['wifi_pw'] = settings.password
config['server'] = settings.BROKER
config['port'] = settings.PORT
config['user'] = settings.MQTT_USER
config['password'] = settings.MQTT_PASS

config["queue_len"] = 1 
config['wifi_coro'] = wifi_han
config['ssl'] = True 

config['ssl_params'] = {"cert_reqs": 0} 

MQTTClient.DEBUG = True  
client = MQTTClient(config)

try:
    asyncio.run(main(client))
finally:
    client.close()
    asyncio.new_event_loop()