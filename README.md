# Proyecto de Ingeniería de Datos con Python

Este proyecto consiste en la implementación de un proceso ETL utilizando diversas tecnologías y bibliotecas de Python. El objetivo es extraer datos de múltiples fuentes, realizar transformaciones sobre ellos y cargar los resultados en una capa Gold y una base de datos MySQL. El proyecto también incluye la programación de tareas automatizadas mediante Apache Airflow.

## Tecnologías utilizadas

- Docker
- Python
- MySQL
- MongoDB
-Cloud Storage (GCP)

## Etapa de Extracción

En esta etapa, se obtiene información de tres fuentes distintas:

- MySQL
- MongoDB
- Cloud Storage

Los datos extraídos se envían a la capa Landing, también conocida como Capa Raw o Bronze, en Cloud Storage.

## Etapa de Transformación con Python

En esta etapa, se toman los datos almacenados en la capa Landing y se aplican diversas transformaciones utilizando las siguientes bibliotecas de Python:

- Pandas
- Polars
- PandaSQL
- Dask

Cada biblioteca se utiliza para crear una transformación específica que responda a una pregunta de negocio planteada, como:

- Ventas de las ciudades, ordenadas de forma descendente.
- Ventas de los productos, ordenadas de forma descendente.
- Ventas por mes y año, ordenadas de la más antigua a la más reciente.
- Ventas de los departamentos del negocio, ordenadas de forma descendente.

Las transformaciones resultantes se cargan en la capa Gold y en una base de datos MySQL.

## Programación de tareas con Apache Airflow

Se ha implementado la programación de tareas automatizadas utilizando Apache Airflow. Esto permite ejecutar el proceso ETL de manera programada y recurrente, garantizando la actualización de los datos en la capa Gold y en la base de datos MySQL.