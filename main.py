import pandas as pd
from sqlalchemy import create_engine
#Conexión a PostgreSQL y MySQL
engine_pg = create_engine("postgresql+psycopg2://scott:tiger@localhost:5432/mydatabase")
engine_my = create_engine("mysql+pymysql://usuario:password@localhost:3306/db_ventas")

#Seleccionar datos de PostgreSQL y MySQL
Query_Clientes = "SELECT * FROM clientes"
Query_Ventas = "SELECT * FROM ventas"

#Extraer datos de PostgreSQL y MySQL
df_Extraer_Clientes = pd.read_sql(Query_Clientes, engine_pg)
df_Extraer_Ventas = pd.read_sql(Query_Ventas, engine_my)

#Eliminar duplicados de los DataFrames
df_delete_duplicados_clientes = df_Extraer_Clientes.drop_duplicates()
df_delete_duplicados_ventas = df_Extraer_Ventas.drop_duplicates()

#Limpieza de datos: eliminar filas con valores nulos
df_limpieza_duplicados_clientes = df_delete_duplicados_clientes.dropna()
df_limpieza_duplicados_ventas = df_delete_duplicados_ventas.dropna()

#Formato Fechas
df_limpieza_duplicados_ventas['Fecha'] = pd.to_datetime(df_limpieza_duplicados_ventas['Fecha'])

#Union de los DataFrames
df_Union_Clientes = pd.merge(
    df_limpieza_duplicados_clientes,
    df_limpieza_duplicados_ventas,
    on='ID_Clientes',
    how='inner'
)

#Metricas Nuevas
df_Total_Gastado = df_Union_Clientes.groupby('ID_Clientes')['Monto'].sum().reset_index()
df_Total_Gastado.rename(columns={'Monto': 'Total_Gastado'}, inplace=True)

# Conteo de transacciones por ciudad
df_conteo_transacciones = df_Union_Clientes.groupby('Ciudad')['ID_Ventas'].count().reset_index()
df_conteo_transacciones.rename(columns={'ID_Ventas': 'Conteo_Transacciones'}, inplace=True)

#Extracion el csv
df_Union_Clientes.to_csv('Clientes_Ventas.csv', index=False)