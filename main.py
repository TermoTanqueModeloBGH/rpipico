# Germán Andrés Xander 2024

from machine import Pin
import dht

d = dht.DHT11(Pin(15))
d.measure()
temperatura=d.temperature()
print(f"\nla temperatura actual es de {temperatura} C")
humedad=d.humidity()
print(f"la humedad actual es de {humedad} %")