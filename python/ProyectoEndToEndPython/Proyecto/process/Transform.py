from pandasql import sqldf
import pandas as pd
import dask.dataframe as dd
import polars as pl

pysqldf = lambda q: sqldf(q, globals())

class Transform():
    def __init__(self) -> None:
        self.process = 'Transform Process'

    def enunciado1(self, order_items_df, orders_df):
        # Convierte los DataFrames de Pandas a Polars
        oi = pl.from_pandas(order_items_df)
        o = pl.from_pandas(orders_df)
        
        # Realiza la unión de los DataFrames
        joined_df = oi.join(o, left_on='order_item_order_id', right_on='order_id')

        joined_df = (
            joined_df.with_columns([
                pl.col('order_date').str.strptime(pl.Date, format='%Y-%m-%d', strict=False)
            ])
        )
        
        df = joined_df.select([
        (pl.col("order_date").dt.year()).alias("Year"),
        (pl.col("order_date").dt.month()).alias("Month"),
        pl.col("order_item_subtotal").alias("Subtotal")
        ])\
            .groupby('Year', 'Month').agg(pl.col('Subtotal').sum().alias('Total'))\
            .sort(by=[pl.col('Year'), pl.col('Month')])\
            .to_pandas()
        
        return df
    
    def enunciado2(self, order_items_df, products_df, categories_df, departments_df):
        # Convertir los dataframes de Pandas a dataframes de Dask
        order_items_ddf = dd.from_pandas(order_items_df, npartitions=1)
        products_ddf = dd.from_pandas(products_df, npartitions=1)
        categories_ddf = dd.from_pandas(categories_df, npartitions=1)
        departments_ddf = dd.from_pandas(departments_df, npartitions=1)
        
        # Realizar el join de los dataframes
        merged_ddf = order_items_ddf.merge(products_ddf, left_on='order_item_product_id', right_on='product_id') \
                                    .merge(categories_ddf, left_on='product_category_id', right_on='category_id') \
                                    .merge(departments_ddf, left_on='category_department_id', right_on='department_id')
        
        # Realizar la operación de groupby y agregación
        result_ddf = merged_ddf.groupby(['product_name', 'category_name', 'department_name']) \
                               .agg({'order_item_quantity': 'sum'}) \
                               .rename(columns={'order_item_quantity': 'Total_Cantidad'})
        
        # Ordenar los resultados por Total_Cantidad de forma descendente
        result_ddf = result_ddf.sort_values('Total_Cantidad', ascending=False)
        
        # Ejecutar la consulta, obtener el resultado y devolver el valor
        result = result_ddf.compute()
        return result

    def enunciado3(self, order_items_df, orders_df, customers_df):
        # Realizar el INNER JOIN de las tablas
        merged_df = pd.merge(order_items_df, orders_df, left_on='order_item_order_id', right_on='order_id')
        merged_df = pd.merge(merged_df, customers_df, left_on='order_customer_id', right_on='customer_id')
        
        # Calcular la suma del order_item_subtotal por customer_city y customer_state
        result = merged_df.groupby(['customer_city', 'customer_state']).agg({'order_item_subtotal': 'sum'})
        result = result.rename(columns={'order_item_subtotal': 'Total_Ventas'}).round(2)

        
        # Ordenar por Total_Ventas de forma descendente
        result = result.sort_values('Total_Ventas', ascending=False)
        
        # Limitar los resultados a los primeros 10 registros
        #result = result.head(10)
        return result

    def enunciado4(self, order_items_df, products_df, categories_df, departments_df):
        q = """
                SELECT
                	category_name, department_name, ROUND(SUM(order_item_subtotal), 2) AS Total_Venta, SUM(order_item_quantity) AS Total_Cantidad
                FROM 
                	order_items_df AS oi
                INNER JOIN
                	products_df AS p
                    ON oi.order_item_product_id = p.product_id
                INNER JOIN
                	categories_df AS c
                    ON p.product_category_id = c.category_id
                INNER JOIN
                	departments_df AS d
                    ON c.category_department_id = d.department_id
                GROUP BY
                	category_name, department_name
                ORDER BY
                	Total_Venta DESC
            """
        result = sqldf(q)
        return result
