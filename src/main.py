import sys
import os
from datetime import datetime
import json

# Importar nuestras clases ETL
from extraccion import Extraccion, Logs
from transformacion import Transformacion
from carga import Carga

class ETLManager:
    def __init__(self, config=None):
        self.config = config or self.get_default_config()
        self.logs = Logs("ETL_MANAGER")
        
        # Componentes ETL
        self.extractor = None
        self.transformador = None
        self.cargador = None
        
        # Datos del proceso
        self.dataframes_extraidos = {}
        self.dataframes_transformados = {}
        self.reporte_verificacion = {}
        
    def get_default_config(self):
        return {
            'mongodb': {
                'host': 'localhost',
                'puerto': 27017,
                'nombre_bd': 'local'  # Cambiado a 'local' que es donde están las colecciones
            },
            'extraccion': {
                'limite_registros': None,  # None para todos los registros
                'colecciones': ['listings', 'reviews']  # Solo las que tienes disponibles
            },
            'carga': {
                'sqlite_path': 'data/airbnb_dw.db',
                'excel_path': 'output/'
            },
            'logs': {
                'nivel': 'INFO'
            }
        }
    
    def validar_configuracion(self):
        try:
            # Validar configuración de MongoDB
            mongodb_config = self.config.get('mongodb', {})
            if not all(key in mongodb_config for key in ['host', 'puerto', 'nombre_bd']):
                self.logs.error("Configuración de MongoDB incompleta")
                return False
            
            # Validar rutas de salida
            carga_config = self.config.get('carga', {})
            if 'sqlite_path' in carga_config:
                os.makedirs(os.path.dirname(carga_config['sqlite_path']), exist_ok=True)
            if 'excel_path' in carga_config:
                os.makedirs(carga_config['excel_path'], exist_ok=True)
            
            self.logs.info("Configuración validada correctamente")
            return True
            
        except Exception as e:
            self.logs.error(f"Error al validar configuración: {str(e)}")
            return False
    
    def inicializar_componentes(self):
        try:
            mongodb_config = self.config['mongodb']
            carga_config = self.config['carga']
            
            # Inicializar extractor
            self.extractor = Extraccion(
                host=mongodb_config['host'],
                puerto=mongodb_config['puerto'],
                nombre_bd=mongodb_config['nombre_bd']
            )
            
            # Inicializar transformador
            self.transformador = Transformacion()
            
            # Inicializar cargador
            self.cargador = Carga(
                ruta_sqlite=carga_config['sqlite_path'],
                ruta_excel=carga_config['excel_path']
            )
            
            self.logs.info("Componentes ETL inicializados correctamente")
            return True
            
        except Exception as e:
            self.logs.error(f"Error al inicializar componentes: {str(e)}")
            return False
    
    def ejecutar_extraccion(self):
        self.logs.info("=== FASE 1: EXTRACCIÓN ===")
        
        try:
            # Conectar a MongoDB
            if not self.extractor.conectar():
                self.logs.error("No se pudo conectar a MongoDB")
                return False
            
            # Obtener estadísticas de la base de datos
            self.extractor.obtener_estadisticas_bd()
            
            # Extraer datos
            limite = self.config['extraccion'].get('limite_registros')
            self.dataframes_extraidos = self.extractor.extraer_todas_colecciones(limite)
            
            # Verificar que se extrajeron algunos datos (al menos una colección con datos)
            datos_extraidos = any(not df.empty for df in self.dataframes_extraidos.values())
            
            if not datos_extraidos:
                self.logs.error("No se extrajeron datos de ninguna colección")
                return False
            
            # Log de lo que se extrajo exitosamente
            for nombre, df in self.dataframes_extraidos.items():
                if not df.empty:
                    self.logs.info(f"Datos extraídos de {nombre}: {len(df)} registros")
                else:
                    self.logs.info(f"Colección {nombre}: Sin datos disponibles")
            
            # Cerrar conexión
            self.extractor.cerrar_conexion()
            
            self.logs.info("Fase de extracción completada exitosamente")
            return True
            
        except Exception as e:
            self.logs.error(f"Error en fase de extracción: {str(e)}")
            return False
    
    def ejecutar_transformacion(self):
        self.logs.info("=== FASE 2: TRANSFORMACIÓN ===")
        
        try:
            # Ejecutar transformaciones
            self.dataframes_transformados = self.transformador.ejecutar_transformacion_completa(
                self.dataframes_extraidos
            )
            
            # Generar reporte de calidad
            reporte_calidad = self.transformador.generar_reporte_calidad()
            
            # Log del reporte de calidad
            for tabla, stats in reporte_calidad.items():
                self.logs.info(f"Calidad {tabla}: {stats['total_registros']} registros")
            
            self.logs.info("Fase de transformación completada exitosamente")
            return True
            
        except Exception as e:
            self.logs.error(f"Error en fase de transformación: {str(e)}")
            return False
    
    def ejecutar_carga(self):
        self.logs.info("=== FASE 3: CARGA ===")
        
        try:
            # Ejecutar carga completa
            self.reporte_verificacion = self.cargador.ejecutar_carga_completa(
                self.dataframes_transformados
            )
            
            self.logs.info("Fase de carga completada exitosamente")
            return True
            
        except Exception as e:
            self.logs.error(f"Error en fase de carga: {str(e)}")
            return False
    
    def generar_reporte_final(self):
        self.logs.info("=== GENERANDO REPORTE FINAL ===")
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        reporte = {
            'proceso_etl': {
                'fecha_ejecucion': timestamp,
                'configuracion': self.config,
                'estado': 'COMPLETADO'
            },
            'extraccion': {
                'colecciones_procesadas': list(self.dataframes_extraidos.keys()),
                'registros_extraidos': {
                    nombre: len(df) for nombre, df in self.dataframes_extraidos.items()
                }
            },
            'transformacion': {
                'colecciones_transformadas': list(self.dataframes_transformados.keys()),
                'registros_transformados': {
                    nombre: len(df) for nombre, df in self.dataframes_transformados.items()
                }
            },
            'carga': {
                'verificacion': self.reporte_verificacion
            }
        }
        
        # Guardar reporte en archivo JSON
        reporte_path = f"output/reporte_etl_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        os.makedirs(os.path.dirname(reporte_path), exist_ok=True)
        
        with open(reporte_path, 'w', encoding='utf-8') as f:
            json.dump(reporte, f, indent=2, ensure_ascii=False)
        
        self.logs.info(f"Reporte final guardado en: {reporte_path}")
        
        # Log resumen del proceso
        total_extraidos = sum(len(df) for df in self.dataframes_extraidos.values())
        total_transformados = sum(len(df) for df in self.dataframes_transformados.values())
        
        self.logs.info("=== RESUMEN FINAL ===")
        self.logs.info(f"Total registros extraídos: {total_extraidos:,}")
        self.logs.info(f"Total registros transformados: {total_transformados:,}")
        self.logs.info(f"Tablas cargadas en SQLite: {len(self.reporte_verificacion)}")
        self.logs.info("Proceso ETL completado exitosamente")
        
        return reporte
    
    def ejecutar_etl_completo(self):
        inicio = datetime.now()
        self.logs.info("=== INICIANDO PROCESO ETL COMPLETO ===")
        self.logs.info(f"Hora de inicio: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Validar configuración
            if not self.validar_configuracion():
                return False
            
            # Inicializar componentes
            if not self.inicializar_componentes():
                return False
            
            # Ejecutar fases del ETL
            if not self.ejecutar_extraccion():
                return False
            
            if not self.ejecutar_transformacion():
                return False
            
            if not self.ejecutar_carga():
                return False
            
            # Generar reporte final
            self.generar_reporte_final()
            
            # Calcular tiempo total
            fin = datetime.now()
            duracion = fin - inicio
            
            self.logs.info("=== PROCESO ETL COMPLETADO EXITOSAMENTE ===")
            self.logs.info(f"Hora de finalización: {fin.strftime('%Y-%m-%d %H:%M:%S')}")
            self.logs.info(f"Duración total: {duracion}")
            
            return True
            
        except Exception as e:
            self.logs.error(f"Error crítico en proceso ETL: {str(e)}")
            return False


def cargar_configuracion_desde_archivo(ruta_config):
    try:
        with open(ruta_config, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Archivo de configuración no encontrado: {ruta_config}")
        return None
    except json.JSONDecodeError:
        print(f"Error al parsear archivo de configuración: {ruta_config}")
        return None


def mostrar_ayuda():
    print("""
=== PROCESO ETL AIRBNB CIUDAD DE MÉXICO ===

Uso:
    python main.py [opciones]

Opciones:
    --config <archivo>    Usar archivo de configuración personalizado
    --limite <numero>     Limitar número de registros a extraer (para testing)
    --help               Mostrar esta ayuda

Ejemplos:
    python main.py                           # Ejecutar con configuración por defecto
    python main.py --limite 1000            # Ejecutar con máximo 1000 registros
    python main.py --config mi_config.json  # Usar configuración personalizada

Requisitos:
    - MongoDB corriendo en localhost:27017
    - Base de datos 'airbnb_mexico' con colecciones: listings, reviews, calendar
    - Librerías: pandas, pymongo, sqlite3, openpyxl

Salidas:
    - Base de datos SQLite: data/airbnb_dw.db
    - Archivos Excel: output/
    - Logs: logs/
    - Reporte: output/reporte_etl_*.json
""")


def main():
    import argparse
    
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(description='Proceso ETL para Airbnb Ciudad de México')
    parser.add_argument('--config', help='Archivo de configuración JSON')
    parser.add_argument('--limite', type=int, help='Límite de registros a extraer')
    parser.add_argument('--help-etl', action='store_true', help='Mostrar ayuda detallada')
    
    args = parser.parse_args()
    
    # Mostrar ayuda si se solicita
    if args.help_etl:
        mostrar_ayuda()
        return
    
    try:
        # Cargar configuración
        config = None
        if args.config:
            config = cargar_configuracion_desde_archivo(args.config)
            if config is None:
                print("Error al cargar configuración, usando configuración por defecto")
        
        # Crear manager ETL
        etl_manager = ETLManager(config)
        
        # Aplicar límite si se especifica
        if args.limite:
            etl_manager.config['extraccion']['limite_registros'] = args.limite
            print(f"Limitando extracción a {args.limite} registros por colección")
        
        # Ejecutar proceso ETL
        exito = etl_manager.ejecutar_etl_completo()
        
        if exito:
            print("\nProceso ETL completado exitosamente!")
            print(f"Revisa los archivos generados en:")
            print(f"   - SQLite: {etl_manager.config['carga']['sqlite_path']}")
            print(f"   - Excel: {etl_manager.config['carga']['excel_path']}")
            print(f"   - Logs: logs/")
            sys.exit(0)
        else:
            print("\nEl proceso ETL falló. Revisa los logs para más detalles.")
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\nProceso ETL interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\nError crítico: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()