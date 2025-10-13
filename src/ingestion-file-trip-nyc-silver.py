import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, year, month
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
import glob

# -----------------------------
# CONFIGURAÇÃO DO LOGGER
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------------
# CONFIGURAÇÕES INICIAIS
# -----------------------------
base_path = "/Volumes/ifood_test/default/nyc_taxi_data/2023/"

# Função genérica para processar Yellow ou Green
def process_taxi_data(taxi_color, schema, table_name):
    path_pattern = base_path + f"{taxi_color}_tripdata_2023-*.parquet"
    file_paths = glob.glob(path_pattern)

    if not file_paths:
        logger.error(f"Nenhum arquivo encontrado para {taxi_color} em {base_path}")
        return None
    else:
        logger.info(f"{len(file_paths)} arquivos {taxi_color.capitalize()} encontrados.")

    all_dfs = []
    for file_path in file_paths:
        try:
            logger.info(f"Iniciando processamento do arquivo: {file_path}")
            df_temp = spark.read.parquet(file_path)

            for field in schema:
                col_name = field.name
                target_type = field.dataType
                if col_name in df_temp.columns:
                    df_temp = df_temp.withColumn(col_name, col(col_name).cast(target_type))
                else:
                    df_temp = df_temp.withColumn(col_name, lit(None).cast(target_type))

            cols_to_select = [field.name for field in schema]
            df_temp = df_temp.select(*cols_to_select)
            df_temp = df_temp.withColumn("type_data", lit(taxi_color).cast(StringType()))
            
            all_dfs.append(df_temp)
            logger.info(f"Arquivo processado com sucesso: {file_path}")

        except Exception as e:
            logger.error(f"ERRO ao processar arquivo {file_path}: {e}", exc_info=True)
    
    if not all_dfs:
        logger.warning(f"Nenhum DataFrame válido foi processado para {taxi_color}.")
        return None

    logger.info(f"Unindo {len(all_dfs)} DataFrames processados para {taxi_color}.")
    df_final = all_dfs[0]
    for i in range(1, len(all_dfs)):
        df_final = df_final.unionByName(all_dfs[i])

    # Preparar DataFrame para gravação
    df_final.createOrReplaceTempView(f"{taxi_color}_taxi_trips")
    pickup_col = "tpep_pickup_datetime" if taxi_color == "yellow" else "lpep_pickup_datetime"
    df_query = spark.sql(f"SELECT * FROM {taxi_color}_taxi_trips WHERE {pickup_col} IS NOT NULL")
    df_source = df_query.withColumn("trip_year", year(col(pickup_col))) \
                        .withColumn("trip_month", month(col(pickup_col)))

    # Construir predicado replaceWhere
    years_months = df_source.select("trip_year", "trip_month").distinct().collect()
    replace_condition = " OR ".join([f"(trip_year={row.trip_year} AND trip_month={row.trip_month})"
                                     for row in years_months])

    logger.info(f"Gravando tabela Delta {table_name} com replaceWhere: {replace_condition}")
    try:
        df_source.write.format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", replace_condition) \
            .partitionBy("trip_year", "trip_month") \
            .saveAsTable(table_name)
        logger.info(f"Tabela Delta {table_name} gravada com sucesso!")
    except Exception as e:
        logger.error(f"ERRO ao gravar tabela Delta {table_name}: {e}", exc_info=True)

# -----------------------------
# DEFINIÇÃO DOS SCHEMAS
# -----------------------------
schema_yellow = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True),
    StructField("type_data", StringType(), False),
])

schema_green = StructType([
    StructField("VendorID", LongType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", LongType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("trip_type", LongType(), True),
    StructField("type_data", StringType(), False),
])

# -----------------------------
# EXECUÇÃO
# -----------------------------
process_taxi_data("yellow", schema_yellow, "ifood_test.default.taxi_trip_yellow")
process_taxi_data("green", schema_green, "ifood_test.default.taxi_trip_green")
