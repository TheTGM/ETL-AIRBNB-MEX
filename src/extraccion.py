import pandas as pd
import pymongo
from pymongo import MongoClient
import logging
from datetime import datetime
import os

class Logs:
    def __init__(self, proceso_nombre="ETL"):
        self.proceso_nombre = proceso_nombre
        self.setup_logger()
    
    def setup_logger(self):
        # Crear directorio logs si no existe
        if not os.path.exists('logs'):
            os.makedirs('logs')
        
        # Nombre del archivo con timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        log_filename = f'logs/log_{timestamp}.txt'
        
        # Configurar logger
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler()  # También mostrar en consola
            ]
        )
        
        self.logger = logging.getLogger(self.proceso_nombre)
        self.logger.info(f"=== Iniciando proceso {self.proceso_nombre} ===")
    
    def info(self, mensaje):
        self.logger.info(mensaje)
    
    def warning(self, mensaje):
        self.logger.warning(mensaje)
    
    def error(self, mensaje):
        self.logger.error(mensaje)


class Extraccion:
    def __init__(self, host='localhost', puerto=27017, nombre_bd='local'):
        self.host = host
        self.puerto = puerto
        self.nombre_bd = nombre_bd
        self.client = None
        self.db = None
        self.logs = Logs("EXTRACCION")
        
    def conectar(self):
        try:
            # Crear cliente MongoDB
            self.client = MongoClient(self.host, self.puerto, serverSelectionTimeoutMS=5000)
            
            # Verificar conexión
            self.client.admin.command('ismaster')
            
            # Seleccionar base de datos
            self.db = self.client[self.nombre_bd]
            
            self.logs.info(f"Conexión exitosa a MongoDB: {self.host}:{self.puerto}/{self.nombre_bd}")
            return True
            
        except Exception as e:
            self.logs.error(f"Error al conectar a MongoDB: {str(e)}")
            return False
    
    def extraer_coleccion(self, nombre_coleccion, limite=None):
        try:
            if self.db is None:
                self.logs.error("No hay conexión a la base de datos")
                return pd.DataFrame()
            
            # Verificar que la colección existe
            if nombre_coleccion not in self.db.list_collection_names():
                self.logs.warning(f"La colección '{nombre_coleccion}' no existe")
                return pd.DataFrame()
            
            coleccion = self.db[nombre_coleccion]
            
            # Contar total de documentos
            total_docs = coleccion.count_documents({})
            self.logs.info(f"Total de documentos en '{nombre_coleccion}': {total_docs}")
            
            # Extraer documentos
            if limite:
                documentos = list(coleccion.find().limit(limite))
                self.logs.info(f"Extrayendo {limite} documentos de '{nombre_coleccion}'")
            else:
                documentos = list(coleccion.find())
                self.logs.info(f"Extrayendo todos los documentos de '{nombre_coleccion}'")
            
            # Convertir a DataFrame
            if documentos:
                df = pd.DataFrame(documentos)
                self.logs.info(f"DataFrame creado: {len(df)} filas, {len(df.columns)} columnas")
                
                # Información básica del DataFrame
                self.logs.info(f"Columnas en '{nombre_coleccion}': {list(df.columns)}")
                
                return df
            else:
                self.logs.warning(f"No se encontraron documentos en '{nombre_coleccion}'")
                return pd.DataFrame()
                
        except Exception as e:
            self.logs.error(f"Error al extraer colección '{nombre_coleccion}': {str(e)}")
            return pd.DataFrame()
    
    def extraer_todas_colecciones(self, limite_por_coleccion=None):
        # Obtener colecciones disponibles en la base de datos
        colecciones_disponibles = self.db.list_collection_names()
        self.logs.info(f"Colecciones disponibles en la BD: {colecciones_disponibles}")
        
        # Filtrar solo las colecciones que nos interesan y que existen
        colecciones_objetivo = ['listings', 'reviews', 'calendar']
        colecciones_a_extraer = [col for col in colecciones_objetivo if col in colecciones_disponibles]
        
        self.logs.info(f"Colecciones a extraer: {colecciones_a_extraer}")
        
        dataframes = {}
        
        self.logs.info("=== Iniciando extracción de colecciones disponibles ===")
        
        for coleccion in colecciones_a_extraer:
            self.logs.info(f"Procesando colección: {coleccion}")
            df = self.extraer_coleccion(coleccion, limite_por_coleccion)
            dataframes[coleccion] = df
            
            if not df.empty:
                self.logs.info(f"✓ {coleccion}: {len(df)} registros extraídos")
            else:
                self.logs.warning(f"✗ {coleccion}: Sin datos extraídos")
        
        # Agregar colecciones faltantes como DataFrames vacíos
        for coleccion in colecciones_objetivo:
            if coleccion not in dataframes:
                dataframes[coleccion] = pd.DataFrame()
                self.logs.info(f"○ {coleccion}: No disponible, agregando DataFrame vacío")
        
        self.logs.info("=== Extracción completada ===")
        return dataframes
    
    def obtener_estadisticas_bd(self):
        try:
            if self.db is None:
                self.logs.error("No hay conexión a la base de datos")
                return {}
            
            estadisticas = {}
            colecciones = self.db.list_collection_names()
            
            self.logs.info("=== Estadísticas de la base de datos ===")
            
            for coleccion in colecciones:
                count = self.db[coleccion].count_documents({})
                estadisticas[coleccion] = count
                self.logs.info(f"{coleccion}: {count:,} documentos")
            
            return estadisticas
            
        except Exception as e:
            self.logs.error(f"Error al obtener estadísticas: {str(e)}")
            return {}
    
    def cerrar_conexion(self):
        """Cierra la conexión a MongoDB"""
        if self.client:
            self.client.close()
            self.logs.info("Conexión a MongoDB cerrada")


# Ejemplo de uso
if __name__ == "__main__":
    # Crear instancia del extractor
    extractor = Extraccion()
    
    # Conectar a la base de datos
    if extractor.conectar():
        # Obtener estadísticas generales
        extractor.obtener_estadisticas_bd()
        
        # Extraer todas las colecciones (limitando a 1000 registros por testing)
        dataframes = extractor.extraer_todas_colecciones(limite_por_coleccion=1000)
        
        # Mostrar información básica de cada DataFrame
        for nombre, df in dataframes.items():
            if not df.empty:
                print(f"\n=== {nombre.upper()} ===")
                print(f"Shape: {df.shape}")
                print(f"Columnas: {list(df.columns)[:5]}...")  # Primeras 5 columnas
                print(df.head(2))
        
        # Cerrar conexión
        extractor.cerrar_conexion()
    else:
        print("No se pudo establecer conexión con MongoDB")