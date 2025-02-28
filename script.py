import json
import psycopg2
import pandas as pd
import os
from datetime import date
import hashlib

#Defincicion de variables globales
HUB= "DELICIAS"
HOSTNAME= "OLT-ACC-MOCOLI-01"
PROVEEDOR= "ZTE"
TIPO_DE_RED= "MASIVO"
TIPO_DE_ZONA= "CERRADA"
TIPO_DE_COBERTURA= "BROWNFIELD"
REGION= "R2"
ZONA= "URB. ARRECIFE + URB. LA PENINSULA + URB. CANTABRIA + URB. LA ENSENADA"
PARROQUIA= "LA PUNTILLA"
FEEDER= "T28"
CLUSTER= "Daniel"
HORIZONTAL_RESIDENCIAL_HPs= 15
HORIZONTAL_COMERCIAL_HPs= 0
VERTICAL_RESIDENCIAL_HPs= 397
VERTICAL_COMERCIAL_HPs= 0
CANTIDAD_DE_EDIFICIOS_PROYECTADOS= 0
EDIF_RESID_PROYECTADOS_HPs= 0
EDIF_COMERCIAL_PROYECTADOS_HPs= 0
SOLARES= 83
HPs_TOTALES= 495

#funcion para hash + clouster
def id_hash_cluster():

    # Obtiene el nombre del cluster desde la variable global
    nombre_cluster = CLUSTER    
    # Convierte la fecha de hoy a formato numérico (días desde 30/12/1899)
    hoy = date.today()
    fecha_numerica = (hoy - date(1899, 12, 30)).days + 1  # +1 porque Excel cuenta desde 1/1/1900    
    # Concatena el nombre y la fecha
    texto = nombre_cluster + str(fecha_numerica)    
    # Genera el hash MD5
    h = hashlib.md5(texto.encode())  # Importante: .encode() para convertir el string a bytes
    # Devuelve el hash como string
    hash_value = h.hexdigest()
    print(f"Hash generado para {nombre_cluster} con fecha {hoy}: {hash_value}")
    return hash_value


#funcion para conectar a la base de datos desde configuracion/conexion.json
def conectar():
    try:
        with open('configuracion/conexion.json') as file:
            data = json.load(file)
            conn_params = data['PostgresSQL']
            conexion = psycopg2.connect(
                host=conn_params['host'],
                database=conn_params['database'],
                user=conn_params['user'],
                password=conn_params['password'],
                port=conn_params['port']
            )
            print('Conexión exitosa')
            return conexion
    except FileNotFoundError:
        print('Error: El archivo configuracion/conexion.json no existe')
        return None
    except Exception as e:
        print(f'Error: {str(e)}')
        return None

#función para exportar excel
def exportar_excel_alcance(datos, ruta_archivo=f"generador/alcance_{CLUSTER}.xlsx"):
    """
    Exporta los datos de la consulta a un archivo Excel en la ruta especificada
    con una fila adicional de totales al final formateada en negrita
    :param datos: Resultados de la consulta SQL
    :param ruta_archivo: Ruta donde se guardará el archivo Excel
    :return: Ruta del archivo si fue exitoso, None si hay error
    """
    try:
        # Asegurar que el directorio existe y es accesible
        os.makedirs(os.path.dirname(ruta_archivo), exist_ok=True)

        # Definir los encabezados
        encabezados = [
            'id', 'hostname', 'nombre', 'zona_cobertura', 'canton',
            'puertos_habilitados', 'hps_liberadas', 'home_passes',
            'business_passes', 'fecha_liberacion', 'hp_horizontal_res',
            'hp_horizontal_com', 'hp_vertical_res', 'hp_vertical_com',
            'edif_res', 'edif_com', 'solares_res', 'tipo_cobertura',
            'region', 'parroquia', 'observacion', 'tipo_red',
            'fecha_liberacion_corp', 'tipo', 'tipo_zona'
        ]

        # Crear DataFrame
        df = pd.DataFrame(datos, columns=encabezados)
        
        # Crear una fila para los totales
        totales = {}
        
        # Usar la fecha actual para los campos de fecha
        hoy = date.today()
        
        # Generar un nuevo hash ID para la fila de totales
        totales['id'] = id_hash_cluster()
        
        # Campos numéricos a sumar
        campos_numericos = [
            'puertos_habilitados', 'hps_liberadas', 'home_passes', 'business_passes',
            'hp_horizontal_res', 'hp_horizontal_com', 'hp_vertical_res', 'hp_vertical_com',
            'edif_res', 'edif_com', 'solares_res'
        ]
        
        # Calcular las sumas de los campos numéricos
        for campo in campos_numericos:
            totales[campo] = df[campo].sum()
        
        # Usar fechas actuales para las fechas
        totales['fecha_liberacion'] = hoy
        totales['fecha_liberacion_corp'] = hoy
        
        # Para el resto de campos, usar valores del último registro
        ultimo_registro = df.iloc[-1] if not df.empty else None
        
        # Asegurarnos que tipo y tipo_zona se copian del último registro
        if ultimo_registro is not None:
            totales['tipo'] = ultimo_registro['tipo']
            totales['tipo_zona'] = TIPO_DE_ZONA
            totales['hostname'] = ultimo_registro['hostname']
            totales['nombre'] = ultimo_registro['nombre']
            totales['zona_cobertura'] = ultimo_registro['zona_cobertura']
            totales['canton'] = ultimo_registro['canton']
            totales['tipo_cobertura'] = ultimo_registro['tipo_cobertura']
            totales['region'] = ultimo_registro['region']
            totales['parroquia'] = ultimo_registro['parroquia']
            totales['observacion'] = ultimo_registro['observacion']
            totales['tipo_red'] = ultimo_registro['tipo_red']
        
        # Agregar la fila de totales al DataFrame
        df_totales = pd.DataFrame([totales])
        df_final = pd.concat([df, df_totales], ignore_index=True)
        
        # Exportar a Excel con formato
        with pd.ExcelWriter(ruta_archivo, engine='openpyxl') as writer:
            df_final.to_excel(writer, index=False, sheet_name='Resultados')
            
            # Obtener la hoja de trabajo
            worksheet = writer.sheets['Resultados']
            
            # Aplicar formato negrita a la última fila
            from openpyxl.styles import Font
            
            # Índice de la última fila (encabezado + datos + total)
            ultima_fila = len(df_final) + 1  # +1 por el encabezado
            
            # Aplicar negrita a todas las celdas de la última fila
            for col in range(1, len(encabezados) + 1):
                celda = worksheet.cell(row=ultima_fila, column=col)
                celda.font = Font(bold=True)
            
        print(f"Archivo Excel con totales exportado en {ruta_archivo}")
        return ruta_archivo
        
    except Exception as e:
        print(f'Error al exportar a Excel: {str(e)}')
        return None

#funcion para sumar los valores de todas las tablas de la bd 
def caso_existencia():
    conexion = conectar()
    if conexion is not None:
        try:
            cursor = conexion.cursor()
            query = f"SELECT * FROM clusters WHERE nombre = '{CLUSTER}'"
            cursor.execute(query)
            result = cursor.fetchall()
            if result:
                print(f"Se encontraron {len(result)} registros para el cluster {CLUSTER}.")
                # Exportar resultados a Excel
                archivo = exportar_excel_alcance(result)
                if archivo:
                    print(f"Consulta el archivo {archivo} para ver los resultados detallados.")
                return True
            return False
        except Exception as e:
            print(f"Error en la consulta: {str(e)}")
            return False
        finally:
            cursor.close()
            conexion.close()
    return False

def caso_liberacion():
    """
    Función para crear un archivo Excel con datos de un nuevo cluster que no existe en la BD.
    Usa las variables globales para llenar los campos y genera un ID único.
    """
    try:
        # Generar el ID único
        id_cluster = id_hash_cluster()
        
        # Fecha actual
        hoy = date.today()
        
        # Crear un registro con los datos de las variables globales
        datos = [{
            'id': id_cluster,
            'hostname': HOSTNAME,
            'nombre': CLUSTER,
            'zona_cobertura': ZONA,
            'canton': 'SAMBORONDON',
            'puertos_habilitados': HPs_TOTALES,
            'hps_liberadas': HPs_TOTALES, 
            'home_passes': HPs_TOTALES - HORIZONTAL_COMERCIAL_HPs - VERTICAL_COMERCIAL_HPs,
            'business_passes': HORIZONTAL_COMERCIAL_HPs + VERTICAL_COMERCIAL_HPs,
            'fecha_liberacion': hoy,  # Fecha actual para liberación
            'hp_horizontal_res': HORIZONTAL_RESIDENCIAL_HPs,
            'hp_horizontal_com': HORIZONTAL_COMERCIAL_HPs,
            'hp_vertical_res': VERTICAL_RESIDENCIAL_HPs,
            'hp_vertical_com': VERTICAL_COMERCIAL_HPs,
            'edif_res': EDIF_RESID_PROYECTADOS_HPs,
            'edif_com': EDIF_COMERCIAL_PROYECTADOS_HPs,
            'solares_res': SOLARES,
            'tipo_cobertura': TIPO_DE_COBERTURA,
            'region': REGION,
            'parroquia': PARROQUIA,
            'observacion': f"Feeder: {FEEDER}, Hub: {HUB}",
            'tipo_red': TIPO_DE_RED,
            'fecha_liberacion_corp': hoy,  # Fecha actual para liberación corporativa
            'tipo': 'N/A',
            'tipo_zona': TIPO_DE_ZONA
        }]
        
        # Crear DataFrame
        df = pd.DataFrame(datos)
        
        # Generar nombre de archivo
        ruta_archivo = f"generador/liberacion_{CLUSTER}.xlsx"
        
        # Asegurar que el directorio existe
        os.makedirs(os.path.dirname(ruta_archivo), exist_ok=True)
        
        # Exportar a Excel con formato
        with pd.ExcelWriter(ruta_archivo, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Liberación')
            
            # Aplicar formato negrita a las celdas (encabezados)
            worksheet = writer.sheets['Liberación']
            from openpyxl.styles import Font
            for col in range(1, len(df.columns) + 1):
                celda = worksheet.cell(row=1, column=col)
                celda.font = Font(bold=True)
                
        print(f"Archivo de liberación creado exitosamente en {ruta_archivo}")
        return ruta_archivo
        
    except Exception as e:
        print(f"Error en caso_liberación: {str(e)}")
        return None

#funcion para comprobar que cluster exista en la bd
def comprobar_existencia():
    conexion = conectar()
    if conexion is not None:
        try:
            cursor = conexion.cursor()
            query = f"SELECT * FROM clusters WHERE nombre = '{CLUSTER}'"
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            conexion.close()
            
            if result:
                print(f"El cluster {CLUSTER} existe en la base de datos.")
                # Se hace llamado a la función caso_existencia
                caso_existencia()
            else:
                print(f"El cluster {CLUSTER} no existe en la base de datos.")
                # Se hace llamado a la función caso_liberacion
                caso_liberacion()
        except Exception as e:
            print(f"Error al comprobar existencia: {str(e)}")
            if 'cursor' in locals() and cursor:
                cursor.close()
            conexion.close()
    else:
        print("No se pudo establecer la conexión con la base de datos.")

if __name__ == '__main__':
    comprobar_existencia()

