from process.Extract import Extract
from process.Transform import Transform
from process.Load import Load
import pandas as pd

extract = Extract()
transform = Transform()
load = Load()

print("INICIA LA EXTRACCIÓN DE DATOS DESDE LA CAPA LANDING")

departments_landing = extract.read_cloud_storage('taller_modulo4', 'landing', 'departments')
categories_landing = extract.read_cloud_storage('taller_modulo4', 'landing', 'categories')
products_landing = extract.read_cloud_storage('taller_modulo4', 'landing', 'products')
customers_landing = extract.read_cloud_storage('taller_modulo4', 'landing', 'customers')
orders_landing = extract.read_cloud_storage('taller_modulo4', 'landing', 'orders')
order_items_landing = extract.read_cloud_storage('taller_modulo4', 'landing', 'order_items')

print("TERMINA LA EXTRACCIÓN DE DATOS DESDE LA CAPA LANDING")

print("INICIA LA TRANSFORMACIÓN DE LOS DATOS")

enunciado1 = transform.enunciado1(order_items_landing, orders_landing)
enunciado2 = transform.enunciado2(order_items_landing, products_landing, categories_landing, departments_landing)
enunciado3 = transform.enunciado3(order_items_landing, orders_landing, customers_landing)
enunciado4 = transform.enunciado4(order_items_landing, products_landing, categories_landing, departments_landing)

print("TERMINA LA TRANSFORMACIÓN DE LOS DATOS")

print("INICIA LA CARGA DE DATOS A LA CAPA GOLD")

load.load_to_cloud_storage('taller_modulo4', 'gold', 'e1_venta_mes', enunciado1)
load.load_to_cloud_storage('taller_modulo4', 'gold', 'e2_cant_vendida_productos', enunciado2)
load.load_to_cloud_storage('taller_modulo4', 'gold', 'e3_ventas_ciudades', enunciado3)
load.load_to_cloud_storage('taller_modulo4', 'gold', 'e4_ventas_departamentos', enunciado4)

print("TERMINA LA CARGA DE DATOS A LA CAPA GOLD")

print("INICIA LA CARGA DE DATOS DE BI A MYSQL")

load.load_to_mysql('bi', 'enun1', enunciado1)
load.load_to_mysql('bi', 'enun2', enunciado2)
load.load_to_mysql('bi', 'enun3', enunciado3)
load.load_to_mysql('bi', 'enun4', enunciado4)

print("TERMINA LA CARGA DE DATOS DE BI A MYSQL")