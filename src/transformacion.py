import pandas as pd
import numpy as np
import re
from datetime import datetime
import ast
import logging
from extraccion import Logs

class Transformacion:
    def __init__(self):
        self.logs = Logs("TRANSFORMACION")
        self.dataframes_transformados = {}
        
    def limpiar_precio(self, precio_str):
        if pd.isna(precio_str) or precio_str == '':
            return 0.0
        
        try:
            # Remover símbolos de moneda y comas
            precio_limpio = re.sub(r'[$,]', '', str(precio_str))
            return float(precio_limpio)
        except (ValueError, TypeError):
            return 0.0
    
    def normalizar_fecha(self, fecha_str):
        if pd.isna(fecha_str):
            return None
        
        try:
            # Si es un diccionario de MongoDB con $date
            if isinstance(fecha_str, dict) and '$date' in fecha_str:
                fecha_str = fecha_str['$date']
            
            # Parsear fecha
            if isinstance(fecha_str, str):
                fecha_dt = pd.to_datetime(fecha_str)
            else:
                fecha_dt = fecha_str
                
            return fecha_dt.strftime('%Y-%m-%d')
        except:
            return None
    
    def derivar_variables_tiempo(self, df, columna_fecha):
        df_temp = df.copy()
        
        # Convertir a datetime si no lo está
        df_temp[columna_fecha] = pd.to_datetime(df_temp[columna_fecha], errors='coerce')
        
        # Derivar variables
        df_temp['año'] = df_temp[columna_fecha].dt.year
        df_temp['mes'] = df_temp[columna_fecha].dt.month
        df_temp['dia'] = df_temp[columna_fecha].dt.day
        df_temp['trimestre'] = df_temp[columna_fecha].dt.quarter
        df_temp['dia_semana'] = df_temp[columna_fecha].dt.dayofweek
        df_temp['nombre_mes'] = df_temp[columna_fecha].dt.month_name()
        
        return df_temp
    
    def categorizar_precios(self, df, columna_precio='price_clean'):
        df_temp = df.copy()
        
        try:
            # Verificar que la columna existe
            if columna_precio not in df_temp.columns:
                self.logs.warning(f"Columna {columna_precio} no existe, creando categoría por defecto")
                df_temp['categoria_precio'] = 'No especificado'
                return df_temp
            
            # Función simple para categorizar precios
            def categorizar_precio_individual(precio):
                try:
                    precio_num = float(precio) if pd.notna(precio) else 0
                    if precio_num <= 500:
                        return 'Económico'
                    elif precio_num <= 1000:
                        return 'Medio'
                    elif precio_num <= 2000:
                        return 'Medio-Alto'
                    elif precio_num <= 5000:
                        return 'Alto'
                    else:
                        return 'Premium'
                except:
                    return 'No especificado'
            
            df_temp['categoria_precio'] = df_temp[columna_precio].apply(categorizar_precio_individual)
            self.logs.info("Precios categorizados exitosamente")
            
        except Exception as e:
            self.logs.warning(f"Error categorizando precios: {str(e)}")
            df_temp['categoria_precio'] = 'No especificado'
        
        return df_temp
    
    def expandir_amenities(self, df, columna_amenities='amenities'):
        df_temp = df.copy()
        
        # Verificar si la columna existe
        if columna_amenities not in df_temp.columns:
            self.logs.warning(f"Columna {columna_amenities} no existe, saltando expansión")
            return df_temp
        
        self.logs.info(f"Procesando columna {columna_amenities}")
        
        # Procesar amenities de manera más segura
        amenities_procesados = []
        
        # Iterar por índices en lugar de valores directos
        for idx in df_temp.index:
            try:
                amenities = df_temp.loc[idx, columna_amenities]
                
                # Verificar si es nulo o vacío de manera segura
                if amenities is None or pd.isna(amenities):
                    amenities_procesados.append([])
                    continue
                
                # Convertir a string para verificar si está vacío
                amenities_str = str(amenities).strip()
                if amenities_str == '' or amenities_str.lower() == 'nan':
                    amenities_procesados.append([])
                    continue
                    
                # Procesar según el tipo
                if isinstance(amenities, str) and amenities_str.startswith('['):
                    try:
                        amenities_list = ast.literal_eval(amenities_str)
                    except:
                        amenities_list = []
                elif isinstance(amenities, list):
                    amenities_list = amenities
                elif isinstance(amenities, str):
                    # Si es un string simple, convertir a lista
                    amenities_list = [amenities_str]
                else:
                    amenities_list = []
                    
                # Limpiar nombres de amenities
                amenities_clean = []
                for a in amenities_list:
                    if a and str(a).strip():
                        clean_amenity = re.sub(r'[^a-zA-Z0-9\s]', '', str(a)).strip()
                        if clean_amenity:
                            amenities_clean.append(clean_amenity)
                
                amenities_procesados.append(amenities_clean)
                
            except Exception as e:
                self.logs.warning(f"Error procesando amenities en fila {idx}: {str(e)}")
                amenities_procesados.append([])
        
        # Agregar la columna procesada
        df_temp['amenities_procesados'] = amenities_procesados
        
        # Crear columnas binarias para amenities más comunes
        amenities_comunes = ['WiFi', 'Kitchen', 'Air conditioning', 'Heating', 
                           'TV', 'Washer', 'Dryer', 'Pool', 'Gym', 'Parking']
        
        for amenity in amenities_comunes:
            col_name = f'amenity_{amenity.lower().replace(" ", "_")}'
            try:
                # Función segura para verificar amenities
                def tiene_amenity(amenities_list):
                    try:
                        if not amenities_list or not isinstance(amenities_list, list):
                            return 0
                        for item in amenities_list:
                            if item and amenity.lower() in str(item).lower():
                                return 1
                        return 0
                    except:
                        return 0
                
                df_temp[col_name] = df_temp['amenities_procesados'].apply(tiene_amenity)
                self.logs.info(f"Columna {col_name} creada exitosamente")
                
            except Exception as e:
                self.logs.warning(f"Error creando columna {col_name}: {str(e)}")
                df_temp[col_name] = 0
        
        self.logs.info("Expansión de amenities completada")
        return df_temp
    
    def transformar_listings(self, df_listings):
        self.logs.info("=== Iniciando transformación de LISTINGS ===")
        
        df = df_listings.copy()
        registros_iniciales = len(df)
        self.logs.info(f"Registros iniciales: {registros_iniciales}")
        
        try:
            # 1. Limpieza de valores nulos críticos
            self.logs.info("Paso 1: Limpieza de valores nulos críticos")
            df = df.dropna(subset=['id', 'latitude', 'longitude'])
            self.logs.info(f"Registros después de eliminar nulos críticos: {len(df)}")
            
            # 2. Eliminar duplicados por ID
            self.logs.info("Paso 2: Eliminación de duplicados")
            df = df.drop_duplicates(subset=['id'])
            self.logs.info(f"Registros después de eliminar duplicados: {len(df)}")
            
            # 3. Normalización de precios
            self.logs.info("Paso 3: Normalización de precios")
            df['price_clean'] = df['price'].apply(self.limpiar_precio)
            self.logs.info("Precios normalizados")
            
            # 4. Conversión de fechas
            self.logs.info("Paso 4: Conversión de fechas")
            fecha_cols = ['host_since', 'calendar_last_scraped', 'last_scraped']
            for col in fecha_cols:
                if col in df.columns:
                    df[f'{col}_clean'] = df[col].apply(self.normalizar_fecha)
                    self.logs.info(f"Fecha {col} normalizada")
            
            # 5. Derivación de variables (categorización de precios)
            self.logs.info("Paso 5: Categorización de precios")
            df = self.categorizar_precios(df, 'price_clean')
            self.logs.info("Precios categorizados")
            
            # 6. Expansión de amenities (solo si existe la columna)
            self.logs.info("Paso 6: Expansión de amenities")
            if 'amenities' in df.columns:
                df = self.expandir_amenities(df, 'amenities')
                self.logs.info("Amenities expandidos")
            else:
                self.logs.info("Columna amenities no encontrada, saltando expansión")
            
            # 7. Normalización de campos categóricos
            self.logs.info("Paso 7: Normalización de campos categóricos")
            categorical_mappings = {
                'room_type': {
                    'Entire home/apt': 'Casa/Apartamento completo',
                    'Private room': 'Habitación privada',
                    'Shared room': 'Habitación compartida',
                    'Hotel room': 'Habitación de hotel'
                },
                'property_type': {
                    'Apartment': 'Apartamento',
                    'House': 'Casa',
                    'Condominium': 'Condominio',
                    'Loft': 'Loft',
                    'Other': 'Otro'
                }
            }
            
            for col, mapping in categorical_mappings.items():
                if col in df.columns:
                    try:
                        def mapear_categoria_seguro(valor):
                            if pd.isna(valor):
                                return 'No especificado'
                            valor_str = str(valor).strip()
                            return mapping.get(valor_str, valor_str)
                        
                        df[f'{col}_normalizado'] = df[col].apply(mapear_categoria_seguro)
                        self.logs.info(f"Columna categórica {col} normalizada correctamente")
                        
                    except Exception as e:
                        self.logs.warning(f"Error normalizando columna categórica {col}: {str(e)}")
                        df[f'{col}_normalizado'] = df[col].astype(str)
            
            # 8. Conversión de booleanos - versión simplificada
            self.logs.info("Paso 8: Conversión de booleanos")
            boolean_cols = ['host_is_superhost', 'host_identity_verified', 'has_availability']
            for col in boolean_cols:
                if col in df.columns:
                    try:
                        # Versión más simple y segura
                        df[f'{col}_bin'] = 0  # Valor por defecto
                        
                        # Procesar fila por fila para evitar problemas de arrays
                        for idx in df.index:
                            valor = df.loc[idx, col]
                            if pd.notna(valor):
                                if isinstance(valor, bool):
                                    df.loc[idx, f'{col}_bin'] = 1 if valor else 0
                                elif isinstance(valor, str):
                                    valor_lower = valor.lower().strip()
                                    if valor_lower in ['t', 'true', '1', 'yes', 'si']:
                                        df.loc[idx, f'{col}_bin'] = 1
                        
                        self.logs.info(f"Columna booleana {col} procesada correctamente")
                        
                    except Exception as e:
                        self.logs.warning(f"Error procesando columna booleana {col}: {str(e)}")
                        df[f'{col}_bin'] = 0
            
            # 9. Limpieza de valores numéricos - versión simplificada
            self.logs.info("Paso 9: Limpieza de valores numéricos")
            numeric_cols = ['accommodates', 'bedrooms', 'beds', 'minimum_nights', 
                           'maximum_nights', 'availability_30', 'availability_60', 
                           'availability_90', 'availability_365']
            
            for col in numeric_cols:
                if col in df.columns:
                    try:
                        df[f'{col}_clean'] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                        self.logs.info(f"Columna numérica {col} procesada correctamente")
                        
                    except Exception as e:
                        self.logs.warning(f"Error procesando columna numérica {col}: {str(e)}")
                        df[f'{col}_clean'] = 0
            
            # 10. Limpieza de campos de texto
            self.logs.info("Paso 10: Limpieza de campos de texto")
            text_cols = ['neighbourhood_cleansed', 'name', 'description']
            for col in text_cols:
                if col in df.columns:
                    try:
                        df[f'{col}_clean'] = df[col].fillna('No especificado').astype(str).str.strip()
                        self.logs.info(f"Columna de texto {col} procesada correctamente")
                        
                    except Exception as e:
                        self.logs.warning(f"Error procesando columna de texto {col}: {str(e)}")
                        df[f'{col}_clean'] = 'No especificado'
            
            registros_finales = len(df)
            self.logs.info(f"Registros finales: {registros_finales}")
            self.logs.info(f"Registros eliminados: {registros_iniciales - registros_finales}")
            
            return df
            
        except Exception as e:
            self.logs.error(f"Error en transformación de listings: {str(e)}")
            self.logs.error(f"Tipo de error: {type(e).__name__}")
            import traceback
            self.logs.error(f"Traceback: {traceback.format_exc()}")
            raise e
    
    def transformar_reviews(self, df_reviews):
        self.logs.info("=== Iniciando transformación de REVIEWS ===")
        
        df = df_reviews.copy()
        registros_iniciales = len(df)
        self.logs.info(f"Registros iniciales: {registros_iniciales}")
        
        # 1. Limpieza de valores nulos críticos
        df = df.dropna(subset=['id', 'listing_id'])
        self.logs.info(f"Registros después de eliminar nulos críticos: {len(df)}")
        
        # 2. Eliminar duplicados
        df = df.drop_duplicates(subset=['id'])
        self.logs.info(f"Registros después de eliminar duplicados: {len(df)}")
        
        # 3. Normalización de fechas
        df['date_clean'] = df['date'].apply(self.normalizar_fecha)
        df = self.derivar_variables_tiempo(df, 'date_clean')
        
        # 4. Limpieza de comentarios
        if 'comments' in df.columns:
            df['comments_clean'] = df['comments'].astype(str).str.strip()
            df['comments_length'] = df['comments_clean'].str.len()
            
            # Análisis básico de sentimiento (simplificado)
            positive_words = ['good', 'great', 'excellent', 'amazing', 'perfect', 'wonderful',
                            'bueno', 'excelente', 'perfecto', 'maravilloso']
            negative_words = ['bad', 'terrible', 'awful', 'poor', 'horrible',
                            'malo', 'terrible', 'horrible', 'pésimo']
            
            df['sentiment_score'] = df['comments_clean'].apply(
                lambda x: sum(1 for word in positive_words if word.lower() in x.lower()) -
                         sum(1 for word in negative_words if word.lower() in x.lower())
            )
        
        # 5. Limpieza de nombres de reviewers
        if 'reviewer_name' in df.columns:
            df['reviewer_name_clean'] = df['reviewer_name'].astype(str).str.strip().str.title()
        
        registros_finales = len(df)
        self.logs.info(f"Registros finales: {registros_finales}")
        self.logs.info(f"Registros eliminados: {registros_iniciales - registros_finales}")
        
        return df
    
    def transformar_calendar(self, df_calendar):
        self.logs.info("=== Iniciando transformación de CALENDAR ===")
        
        df = df_calendar.copy()
        registros_iniciales = len(df)
        self.logs.info(f"Registros iniciales: {registros_iniciales}")
        
        # 1. Limpieza de valores nulos críticos
        df = df.dropna(subset=['listing_id', 'date'])
        self.logs.info(f"Registros después de eliminar nulos críticos: {len(df)}")
        
        # 2. Normalización de fechas
        df['date_clean'] = df['date'].apply(self.normalizar_fecha)
        df = self.derivar_variables_tiempo(df, 'date_clean')
        
        # 3. Normalización de precios
        if 'price' in df.columns:
            df['price_clean'] = df['price'].apply(self.limpiar_precio)
        
        # 4. Conversión de disponibilidad
        if 'available' in df.columns:
            df['available_bin'] = df['available'].map({'t': 1, 'f': 0, True: 1, False: 0}).fillna(0)
        
        registros_finales = len(df)
        self.logs.info(f"Registros finales: {registros_finales}")
        
        return df
    
    def ejecutar_transformacion_completa(self, dataframes_extraidos):
        self.logs.info("=== INICIANDO TRANSFORMACIÓN COMPLETA ===")
        
        # Transformar cada DataFrame
        if 'listings' in dataframes_extraidos and not dataframes_extraidos['listings'].empty:
            self.dataframes_transformados['listings'] = self.transformar_listings(dataframes_extraidos['listings'])
        
        if 'reviews' in dataframes_extraidos and not dataframes_extraidos['reviews'].empty:
            self.dataframes_transformados['reviews'] = self.transformar_reviews(dataframes_extraidos['reviews'])
        
        if 'calendar' in dataframes_extraidos and not dataframes_extraidos['calendar'].empty:
            self.dataframes_transformados['calendar'] = self.transformar_calendar(dataframes_extraidos['calendar'])
        
        # Resumen de transformaciones
        self.logs.info("=== RESUMEN DE TRANSFORMACIONES ===")
        for nombre, df in self.dataframes_transformados.items():
            self.logs.info(f"{nombre}: {len(df)} registros, {len(df.columns)} columnas")
        
        return self.dataframes_transformados
    
    def generar_reporte_calidad(self):
        reporte = {}
        
        for nombre, df in self.dataframes_transformados.items():
            reporte[nombre] = {
                'total_registros': len(df),
                'total_columnas': len(df.columns),
                'valores_nulos_por_columna': df.isnull().sum().to_dict(),
                'porcentaje_completitud': ((df.count() / len(df)) * 100).round(2).to_dict()
            }
        
        self.logs.info("Reporte de calidad generado")
        return reporte


# Ejemplo de uso
if __name__ == "__main__":
    # Simular datos de prueba
    from extraccion import Extraccion
    
    # Crear instancia del transformador
    transformador = Transformacion()
    
    # Aquí normalmente vendrían los datos del extractor
    # transformador.ejecutar_transformacion_completa(dataframes_extraidos)
    
    print("Clase Transformacion lista para usar")