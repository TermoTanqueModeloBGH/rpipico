from machine import Pin
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

#Para el destello
evento=asyncio.Event()

#Aca esta el sensor
d = dht.DHT11(machine.Pin(15)) 
#el led de destello
led=machine.Pin("LED", Pin.OUT)
#pin para el rele   
r=machine.Pin(16, Pin.OUT)
r.value(1)

#inicializacion y valores default o guardados en un JSON
try:
    with open("estado.json","r") as f:
        estado=json.load(f)
    print("datos almacenados cargados")
except OSError as e:
    print("sin datos guardados\nse guardan los de default")
    estado = {
    "setpoint": 25.0,
    "modo": "auto", #puede ser "auto" o "manual"
    "periodo": 25,
    "rele_orden": False  # lo que se recibe por el topico rele
    }

#Guardado de datos
def guardar_datos():  
    try:
        with open("estado.json", "w") as f:
            json.dump(estado, f)
    except OSError:
        print("Error guardando en flash")

#Para recibir los datos del MQTTX
async def messages(client):
    async for topic, msg, retained in client.queue:
        
        instruccion=topic.decode()
        valor=msg.decode()

        band=False #bandera para guardar solo si hubo algun cambio

        print(f"comando recibido: {instruccion}->{valor}")

        if instruccion.endswith('/setpoint'):
            estado["setpoint"]=float(valor)
            band=True
        elif instruccion.endswith('/periodo'):
            estado["periodo"]=int(valor)
            band=True
        elif instruccion.endswith('/modo'):
            estado["modo"]=valor.lower() # 'auto' o 'manual'
            band=True
        elif instruccion.endswith('/rele'):
            estado["rele_orden"]=valor == "1"
            band=True
        elif instruccion.endswith('/destello'):
            evento.set() #se activa el evento del destello
            band=True
        if band == True:
           guardar_datos()
async def destello():
    while True:
        await evento.wait()
        evento.clear()
        for _ in range(10):
            led.toggle()
            await asyncio.sleep_ms(200)
        led.off()


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
    asyncio.create_task(up(client))
    asyncio.create_task(messages(client))
    asyncio.create_task(destello())
    while True:
        try:
            d.measure()
            temperatura=d.temperature()
            humedad=d.humidity()

            if estado["modo"] =="auto":
                if temperatura>estado["setpoint"]:
                    r.value(0) #se enciende porque es activo en bajo
                else:
                    r.value(1) #apagar
            else:
                if estado["rele_orden"]:
                    r.value(0)
                else:
                    r.value(1)
    
            datos= {
                "temperatura":temperatura,
                "humedad":humedad,
                "setpoint": estado["setpoint"],
                "periodo": estado["periodo"],
                "modo": estado["modo"]
            
            }
            conversion=json.dumps(datos)
            await client.publish(ID_DEL_DISPOSITIVO, conversion, qos = 1)
        except OSError as e:
            print("sin sensor")
        await asyncio.sleep(estado["periodo"])  # manda cada cuanto

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
