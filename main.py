from machine import Pin
from mqtt_as import MQTTClient
from mqtt_as import config
import settings
import uasyncio as asyncio
import dht, machine, json
import network
import ubinascii


wlan = network.WLAN(network.STA_IF)
wlan.active(True)
ID_DEL_DISPOSITIVO = ubinascii.hexlify(wlan.config('mac'), ':').decode()
print('\nMAC del dispositivo:', ID_DEL_DISPOSITIVO)


d = dht.DHT11(machine.Pin(15)) 
r = machine.Pin(16, Pin.OUT)
r.value(1) # Relé apagado por defecto (Lógica inversa)

# Estado por default del dispositivo
estado = {
    "modo": "manual", 
    "periodo": 60, 
    "rele_orden": False  
}

# Recepción de mensajes MQTT
async def messages(client):
    async for topic, msg, retained in client.queue:
        instruccion = topic.decode()
        valor = msg.decode()

        print(f"Comando recibido en {instruccion} -> {valor}")

        if instruccion == 'nodo/comando':
            estado["rele_orden"] = (valor == "true")
            estado["modo"] = "manual"
            
            # Control físico del relé
            if estado["rele_orden"]:
                r.value(0) # Enciende
            else:
                r.value(1) # Apaga
                
            # Publicación inmediata del estado para actualizar el ui-led de Node-RED
            estado_led = "true" if r.value() == 0 else "false"
            await client.publish("nodo/estado", estado_led, qos=1)

#Estado de la conexión WiFi
async def wifi_han(state):
    print('WiFi conectado' if state else 'WiFi desconectado')
    await asyncio.sleep(1)

# Suscripción a tópicos 
async def up(client):
    while True:
        await client.up.wait()
        client.up.clear()
        print("Conectado al Broker. Suscribiendo a tópicos...")
        await client.subscribe('nodo/comando', 1)
        await client.subscribe(f'{ID_DEL_DISPOSITIVO}/#', 1)

# Lectura cada 60 segundos bucle
async def main(client):
    print(f"Intentando conectar al broker en {settings.BROKER}...")
    await client.connect()
    
    asyncio.create_task(up(client))
    asyncio.create_task(messages(client))
    
    while True:
        try:
            # Lectura periódica del sensor
            d.measure()
            temperatura = d.temperature()
            humedad = d.humidity()
            
            print(f"Lectura - Temp: {temperatura}°C, Hum: {humedad}%")
        
            datos = {"temperatura": temperatura, "humedad": humedad}
            await client.publish(ID_DEL_DISPOSITIVO, json.dumps(datos), qos=1)
            
            estado_led = "true" if r.value() == 0 else "false"
            await client.publish("nodo/estado", estado_led, qos=1)
        except OSError:
            print("Error al leer el sensor DHT11.")
        await asyncio.sleep(estado["periodo"])  

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
except KeyboardInterrupt:
    print("Ejecución detenida por el usuario.")
finally:
    client.close()
    asyncio.new_event_loop()