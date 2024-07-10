import pandas as pd
from sqlalchemy import create_engine
import time 

#extracción-transformación-carga
def etl():
    df_bip = pd.read_excel('../data/metro_bip.xlsx', sheet_name='Abiertos', skiprows=7)
    df_bip.drop(columns=["ENTIDAD", "HORARIO REFERENCIAL"], inplace=True)

    df_bip.rename(columns={
                    'CODIGO':'linea', 
                    'NOMBRE FANTASIA':'estacion',
                    'DIRECCION': 'direccion',
                    'COMUNA': 'comuna',
                    'ESTE': 'este',
                    'NORTE': 'norte',
                    'LONGITUD': 'longitud',
                    'LATITUD': 'latitud'}, inplace=True)
    #guarda el dataframe en un archivo csv
    df_bip.to_csv('../result/centros_bip.csv', index=False)
    return df_bip

#genera un documento excel, 1º hoja todos los datos, 2º ,3º...nº hoja, datos por comuna o línea
def data_to_excel(df_bip, variable ,excel_name):
    excel_path = '../result/'
    excel_path_name = f'{excel_path}{excel_name}'
    list_variable = df_bip[variable].unique().tolist()
    with pd.ExcelWriter(excel_path_name) as writer:
        df_bip.to_excel(writer, sheet_name = 'DATA', index=False)
        for x in range(len(list_variable)):
            query = df_bip[df_bip[variable] == list_variable[x]]
            sheet_name = '{}'.format(list_variable[x])
            query.to_excel(writer, sheet_name = sheet_name, index=False)
    print(f"Data Loaded: {variable} in {excel_name}")

#guarda los registros en una base de datos postgres
def data_to_postgresql():
    df_bip = pd.read_csv('../result/centros_bip.csv')
    #credenciales de conexión
    user = 'your user (postgres)'
    password = "your password"
    host = 'your host (localhost)'
    port = 'your port (5432)'
    database = 'your database name'
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{database}'
    )
    
    conn = None
    
    try:
        conn = engine.connect()
        #agrega los datos a la tabla
        df_bip.to_sql(
            name = "your table name",
            con = engine,
            if_exists= "append",
            index= False
        )
        print(f"Data Loaded in PostgreSQl")
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            conn.close()
            print("Connection Close")
    
if __name__ == "__main__":
    
    start_time = time.time()

    df_bip = etl()
    data_to_excel(df_bip=df_bip, variable="linea", excel_name="bip_lineas.xlsx")
    data_to_excel(df_bip=df_bip,variable="comuna",excel_name="bip_comunas.xlsx")
    #data_to_postgresql()

    end_time = time.time()
    total_time = end_time - start_time
    print(f"ETL time: {total_time} seconds")
    