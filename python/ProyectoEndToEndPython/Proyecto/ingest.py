from process.Extract import Extract
from process.Load import Load
import pandas as pd

extract = Extract()
load = Load()

print("INICIA LA EXTRACCIÓN DE DATOS")

order_items_df = extract.read_mysql('retail_db', 'order_items')
orders_df = extract.read_mysql('retail_db', 'orders')
customers_df = extract.read_cloud_storage('ducketdep12', 'retail', 'customers')
products_df = extract.read_cloud_storage('ducketdep12', 'retail', 'products')
categories_df = extract.read_mongodb('retail_db', 'categories').drop(['_id'], axis=1)
departments_df = extract.read_mongodb('retail_db', 'departments').drop(['_id'], axis=1)

print("TERMINA LA EXTRACCIÓN DE DATOS")

print("INICIA LA CARGA DE DATOS A LA CAPA LANDING")

load.load_to_cloud_storage('taller_modulo4', 'landing', 'departments', departments_df)
load.load_to_cloud_storage('taller_modulo4', 'landing', 'categories', categories_df)
load.load_to_cloud_storage('taller_modulo4', 'landing', 'products', products_df)
load.load_to_cloud_storage('taller_modulo4', 'landing', 'customers', customers_df)
load.load_to_cloud_storage('taller_modulo4', 'landing', 'orders', orders_df)
load.load_to_cloud_storage('taller_modulo4', 'landing', 'order_items', order_items_df)

print("TERMINA LA CARGA DE DATOS A LA CAPA LANDING")