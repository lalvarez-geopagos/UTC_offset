import requests
import pyodbc as db
import os
import time
from dotenv import load_dotenv 

load_dotenv()  # Carga automáticamente el archivo .env

api_key = '3aa6712010d64ae6a03f02d61f23bf77' #Esta clave se genera al crear un usuario en la web: https://app.abstractapi.com/

def get_connection():
   # Conexión a la base de datos
   return db.connect(
                    f"Driver={{ODBC Driver 17 for SQL Server}};"
                    f"Server={os.getenv('DB_SERVER')};"
                    f"Database={os.getenv('DB_NAME')};"
                    f"UID={os.getenv('DB_USER')};"
                    f"PWD={os.getenv('DB_PASS')};"
                    "Trusted_Connection=no;"
                    "Integrated_Security=false;"
                    )

def get_location (conn):
    cursor = conn.cursor()
    cursor.execute("SELECT distinct location from Timezones")
    location = [row[0] for row in cursor.fetchall()]
    
    location = [x for x in location if x is not None]  #Este paso está aqui sólo para eliminar nulos si los hubiese
    return location

def update_utc_offset (conn, locations):
    cursor = conn.cursor()

    for location in locations:
        try:
            response = requests.get(f"https://timezone.abstractapi.com/v1/current_time/?api_key={api_key}&location={location}")
            result = response.json()
            utc_diff = result.get("gmt_offset")
            
            cursor.execute(f"SELECT dif from Timezones where location = '{location}'")
            actual_dif = cursor.fetchone()[0]

            print(f''' UPDATE Timezones
                                     SET dif = {utc_diff}
                                     WHERE location = '{location}'
                                 ''')

            if actual_dif != utc_diff:
                cursor.execute(f''' UPDATE Timezones
                                    SET dif = {utc_diff}
                                    WHERE location = '{location}'
                                ''')
                
                conn.commit()
                print(f'''Se ha actualizado el offset_UTC para los partner con location: {location}.
                          offset_UTC anterior: {actual_dif} 
                          offset_UTC nuevo: {utc_diff} 
                      ''')
                time.sleep(1)  # Espera 1 segundo para asegurar no incumplir con las normas de la api de 1 solicitud x segundo máx.

        except Exception as e:
            print (f"""Error al intentar actualizar el offset UTC de la timezone: {location}.
                       Type Error: {e}""")
    
    cursor.close()
              
def main():
    try:
        step = 1 # Conexión a SQL-Server / bi-test
        connect = get_connection()
        step = 2 #Obtengo timezones
        locations = get_location (connect) 
        print(locations)
        step = 3 # Actualizo los timezones
        update_utc_offset(connect, locations) 
    
    except Exception as e:      
        print(e)

main()
