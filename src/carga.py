import pandas as pd
import sqlite3
import os
from datetime import datetime
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font, PatternFill
from extraccion import Logs

class Carga:
    
    def __init__(self, ruta_sqlite='data/airbnb_dw.db', ruta_excel='output/'):
        self.ruta_sqlite = ruta_sqlite
        self.ruta_excel = ruta_excel
        self.logs = Logs("CARGA")
        
        # Crear directorios si no existen
        os.makedirs(os.path.dirname(self.ruta_sqlite), exist_ok=True)
        os.makedirs(self.ruta_excel, exist_ok=True)
        
        self.logs.info(f"Carga inicializada - SQLite: {self.ruta_sqlite}, Excel: {self.ruta_excel}")
    
    def cargar_a_sqlite(self, dataframes_transformados):
        self.logs.info("=== INICIANDO CARGA A SQLITE ===")
        
        try:
            with sqlite3.connect(self.ruta_sqlite) as conn:
                for nombre, df in dataframes_transformados.items():
                    if not df.empty:
                        # Crear una copia del DataFrame para no modificar el original
                        df_limpio = df.copy()
                        
                        # Eliminar columnas problemáticas de MongoDB
                        columnas_a_eliminar = ['_id']
                        for col in columnas_a_eliminar:
                            if col in df_limpio.columns:
                                df_limpio = df_limpio.drop(columns=[col])
                                self.logs.info(f"Columna '{col}' eliminada para compatibilidad con SQLite")
                        
                        # Convertir cualquier ObjectId restante a string
                        for col in df_limpio.columns:
                            if df_limpio[col].dtype == 'object':
                                try:
                                    # Convertir posibles ObjectIds a string
                                    df_limpio[col] = df_limpio[col].astype(str)
                                except:
                                    pass
                        
                        # Cargar DataFrame a SQLite
                        tabla_nombre = f"raw_{nombre}_transformado"
                        df_limpio.to_sql(tabla_nombre, conn, if_exists='replace', index=False)
                        self.logs.info(f"Tabla '{tabla_nombre}' cargada: {len(df_limpio)} registros, {len(df_limpio.columns)} columnas")
                    else:
                        self.logs.warning(f"DataFrame '{nombre}' está vacío, saltando carga")
            
            self.logs.info("Carga a SQLite completada exitosamente")
            
        except Exception as e:
            self.logs.error(f"Error en carga a SQLite: {str(e)}")
            raise
    
    def exportar_a_excel(self, dataframes_transformados):
        self.logs.info("=== INICIANDO EXPORTACIÓN A EXCEL ===")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        
        try:
            for nombre, df in dataframes_transformados.items():
                if df.empty:
                    self.logs.warning(f"DataFrame '{nombre}' está vacío, saltando exportación")
                    continue
                
                archivo_excel = os.path.join(self.ruta_excel, f"{nombre}_transformado_{timestamp}.xlsx")
                
                # Exportar de manera simple usando pandas
                with pd.ExcelWriter(archivo_excel, engine='openpyxl') as writer:
                    # Escribir datos principales
                    df.to_excel(writer, sheet_name='Datos', index=False)
                    
                    # Crear hoja de resumen
                    resumen_data = {
                        'Métrica': ['Total de registros', 'Total de columnas', 'Fecha de exportación'],
                        'Valor': [len(df), len(df.columns), datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
                    }
                    
                    df_resumen = pd.DataFrame(resumen_data)
                    df_resumen.to_excel(writer, sheet_name='Resumen', index=False)
                
                self.logs.info(f"Archivo Excel creado: {archivo_excel}")
                
        except Exception as e:
            self.logs.error(f"Error en exportación a Excel: {str(e)}")
            raise
    
    def verificar_carga(self):
        self.logs.info("=== VERIFICANDO INTEGRIDAD DE CARGA ===")
        
        verificacion = {}
        
        try:
            with sqlite3.connect(self.ruta_sqlite) as conn:
                # Obtener lista de tablas
                cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tablas = [row[0] for row in cursor.fetchall()]
                
                for tabla in tablas:
                    # Contar registros
                    cursor = conn.execute(f"SELECT COUNT(*) FROM [{tabla}]")
                    count = cursor.fetchone()[0]
                    
                    verificacion[tabla] = {
                        'registros': count
                    }
                    
                    self.logs.info(f"Tabla '{tabla}': {count} registros")
            
            return verificacion
            
        except Exception as e:
            self.logs.error(f"Error en verificación: {str(e)}")
            return {}
    
    def ejecutar_carga_completa(self, dataframes_transformados):
        self.logs.info("=== INICIANDO CARGA COMPLETA ===")
        
        try:
            # Cargar a SQLite
            self.cargar_a_sqlite(dataframes_transformados)
            
            # Exportar a Excel
            self.exportar_a_excel(dataframes_transformados)
            
            # Verificar carga
            reporte_verificacion = self.verificar_carga()
            
            self.logs.info("=== CARGA COMPLETA FINALIZADA ===")
            return reporte_verificacion
            
        except Exception as e:
            self.logs.error(f"Error en carga completa: {str(e)}")
            raise


# Ejemplo de uso
if __name__ == "__main__":
    # Crear instancia del cargador
    cargador = Carga()
    print("Clase Carga lista para usar")