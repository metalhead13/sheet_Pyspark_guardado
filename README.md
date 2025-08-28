# ============================================================================
# PYSPARK - MÉTODOS DE GUARDADO COMPLETOS CON EJEMPLOS
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Inicializar Spark
spark = SparkSession.builder.appName("GuardadoEjemplos").getOrCreate()

# Crear DataFrame de ejemplo
data = [
    ("Juan", 25, "Madrid", "2024-01-15"),
    ("María", 30, "Barcelona", "2024-01-16"),
    ("Carlos", 28, "Valencia", "2024-01-17"),
    ("Ana", 32, "Sevilla", "2024-01-18")
]
columns = ["nombre", "edad", "ciudad", "fecha"]
df = spark.createDataFrame(data, columns)

# ============================================================================
# 1. GUARDADO BÁSICO EN DIFERENTES FORMATOS
# ============================================================================

# PARQUET (Recomendado para big data)
df.write \
  .mode("overwrite") \
  .parquet("s3://mi-bucket/datos/parquet/")

# CSV
df.write \
  .mode("overwrite") \
  .option("header", "true") \
  .option("delimiter", ";") \
  .csv("s3://mi-bucket/datos/csv/")

# JSON
df.write \
  .mode("overwrite") \
  .json("s3://mi-bucket/datos/json/")

# DELTA (Para data lakes modernos)
df.write \
  .format("delta") \
  .mode("overwrite") \
  .save("s3://mi-bucket/datos/delta/")

# AVRO
df.write \
  .format("avro") \
  .mode("overwrite") \
  .save("s3://mi-bucket/datos/avro/")

# ============================================================================
# 2. MODOS DE ESCRITURA (.mode())
# ============================================================================

# OVERWRITE: Sobrescribe completamente
df.write.mode("overwrite").parquet("path/")

# APPEND: Añade al final
df.write.mode("append").parquet("path/")

# IGNORE: No hace nada si ya existe
df.write.mode("ignore").parquet("path/")

# ERROR/ERRORIFEXISTS: Lanza error si existe (por defecto)
df.write.mode("error").parquet("path/")

# ============================================================================
# 3. PARTICIONADO (.partitionBy())
# ============================================================================

# Particionar por una columna
df.write \
  .mode("overwrite") \
  .partitionBy("ciudad") \
  .parquet("s3://mi-bucket/particionado_ciudad/")

# Particionar por múltiples columnas
df.write \
  .mode("overwrite") \
  .partitionBy("ciudad", "edad") \
  .parquet("s3://mi-bucket/particionado_multiple/")

# Ejemplo con fecha (muy común)
df_fechas = df.withColumn("año", col("fecha").substr(1, 4)) \
             .withColumn("mes", col("fecha").substr(6, 2))

df_fechas.write \
  .mode("overwrite") \
  .partitionBy("año", "mes") \
  .parquet("s3://mi-bucket/particionado_fecha/")

# ============================================================================
# 4. BUCKETING (.bucketBy())
# ============================================================================

# Distribución por buckets (útil para joins futuros)
df.write \
  .mode("overwrite") \
  .bucketBy(4, "edad") \
  .sortBy("nombre") \
  .saveAsTable("tabla_buckets")

# ============================================================================
# 5. OPCIONES ESPECÍFICAS POR FORMATO
# ============================================================================

# === CSV OPTIONS ===
df.write \
  .mode("overwrite") \
  .option("header", "true") \
  .option("delimiter", ";") \
  .option("quote", '"') \
  .option("escape", "\\") \
  .option("nullValue", "NULL") \
  .option("emptyValue", "") \
  .option("dateFormat", "yyyy-MM-dd") \
  .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
  .csv("s3://mi-bucket/csv_opciones/")

# === PARQUET OPTIONS ===
df.write \
  .mode("overwrite") \
  .option("compression", "snappy") \
  .option("mergeSchema", "true") \
  .parquet("s3://mi-bucket/parquet_opciones/")

# === JSON OPTIONS ===
df.write \
  .mode("overwrite") \
  .option("compression", "gzip") \
  .option("dateFormat", "yyyy-MM-dd") \
  .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
  .json("s3://mi-bucket/json_opciones/")

# ============================================================================
# 6. COMPRESIÓN
# ============================================================================

# Diferentes tipos de compresión
compressions = ["none", "snappy", "gzip", "lzo", "brotli", "lz4", "zstd"]

# Para Parquet
df.write.option("compression", "snappy").parquet("path/")

# Para CSV
df.write.option("compression", "gzip").csv("path/")

# Para JSON
df.write.option("compression", "brotli").json("path/")

# ============================================================================
# 7. CONTROL DE ARCHIVOS DE SALIDA
# ============================================================================

# Controlar número de archivos de salida
df.coalesce(1).write.mode("overwrite").parquet("s3://mi-bucket/un_archivo/")

# Reparticionar antes de guardar
df.repartition(4).write.mode("overwrite").parquet("s3://mi-bucket/4_archivos/")

# Reparticionar por columna
df.repartition("ciudad").write.mode("overwrite").parquet("s3://mi-bucket/rep_ciudad/")

# ============================================================================
# 8. GUARDADO EN BASES DE DATOS
# ============================================================================

# JDBC (PostgreSQL, MySQL, etc.)
df.write \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://localhost:5432/midb") \
  .option("dbtable", "mi_tabla") \
  .option("user", "usuario") \
  .option("password", "contraseña") \
  .option("driver", "org.postgresql.Driver") \
  .mode("overwrite") \
  .save()

# ============================================================================
# 9. EJEMPLOS PRÁCTICOS COMUNES
# ============================================================================

# Ejemplo 1: Guardado optimizado para analítica
df.coalesce(4) \
  .write \
  .mode("overwrite") \
  .option("compression", "snappy") \
  .partitionBy("ciudad") \
  .parquet("s3://analytics-bucket/usuarios/")

# Ejemplo 2: CSV para exportar a Excel
df.coalesce(1) \
  .write \
  .mode("overwrite") \
  .option("header", "true") \
  .option("delimiter", ";") \
  .csv("s3://exports-bucket/reporte_usuarios/")

# Ejemplo 3: Guardado incremental por fecha
from datetime import datetime
fecha_actual = datetime.now().strftime("%Y%m%d")

df.write \
  .mode("append") \
  .parquet(f"s3://data-lake/tabla_principal/fecha={fecha_actual}/")

# Ejemplo 4: Guardado con múltiples formatos
base_path = "s3://mi-bucket/multi_formato"

# Parquet para analítica
df.write.mode("overwrite").parquet(f"{base_path}/parquet/")

# CSV para reportes
df.write.mode("overwrite").option("header", "true").csv(f"{base_path}/csv/")

# JSON para APIs
df.write.mode("overwrite").json(f"{base_path}/json/")

# ============================================================================
# 10. MANEJO DE ERRORES Y VALIDACIONES
# ============================================================================

def guardar_con_validacion(dataframe, path, formato="parquet"):
    try:
        if dataframe.count() == 0:
            print("Warning: DataFrame vacío")
            return
        
        if formato == "parquet":
            dataframe.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(path)
        elif formato == "csv":
            dataframe.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(path)
        
        print(f"Datos guardados exitosamente en {path}")
        
    except Exception as e:
        print(f"Error al guardar: {str(e)}")

# Uso
guardar_con_validacion(df, "s3://mi-bucket/datos_validados/", "parquet")

# ============================================================================
# 11. CONFIGURACIONES AVANZADAS
# ============================================================================

# Configurar Spark para optimizar escritura
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

# Para S3, configuraciones adicionales
spark.conf.set("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB
spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")

print("🚀 Ejemplos de guardado con PySpark completados!")
