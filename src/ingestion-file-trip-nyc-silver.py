from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

# Inicialize a SparkSession (ajuste se necessário)
# spark = SparkSession.builder.appName("YellowTripSchemaFix").getOrCreate()

base_path = "/Volumes/ifood_test/default/nyc_taxi_data/2023/"

# Você pode listar os arquivos manualmente ou usar glob:
import glob
file_paths = glob.glob(base_path + "yellow_tripdata_2023-*.parquet")

if not file_paths:
    raise FileNotFoundError(f"Nenhum arquivo encontrado em {base_path} com o padrão 'yellow_tripdata_2023-*.parquet'")

# 2. Definição do Schema de Destino
# Criamos um schema unificado com os tipos MAIS SEGUROS.
# Usamos DOUBLE para tudo que é valor monetário/contagem e BIGINT para IDs.

# NOTA: TIMESTAMP_NTZ é o tipo do Spark para Timestamp sem fuso horário.
# Se for usar em sistemas mais antigos, pode ser que precise mudar para TimestampType()
final_schema = StructType([
    StructField("VendorID", LongType(), True), # Escolhemos BIGINT/LongType (o maior)
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True), # FORÇAMOS DOUBLE (o mais seguro)
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True), # FORÇAMOS DOUBLE (o mais seguro)
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True), # Escolhemos BIGINT/LongType (o maior)
    StructField("DOLocationID", LongType(), True), # Escolhemos BIGINT/LongType (o maior)
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
    StructField("type_data", StringType(), False), # Não nulo, pois será sempre "yellow"
    # Adicione aqui qualquer outra coluna que possa aparecer
])


# 3. e 4. Leitura, CAST e União
all_dfs = []
for file_path in file_paths:
    print(f"Processando arquivo: {file_path}")
    
    # Lemos o arquivo Parquet com o schema original
    df_temp = spark.read.parquet(file_path)
    
    # Aplicamos o CAST para o schema final, forçando a conversão
    for field in final_schema:
        col_name = field.name
        target_type = field.dataType
        
        # Só aplica o cast se a coluna existir no DataFrame atual
        if col_name in df_temp.columns:
            # Garante que o tipo seja o definido no schema final
            df_temp = df_temp.withColumn(col_name, col(col_name).cast(target_type))
        else:
            # Caso a coluna não exista no arquivo (menos provável, mas seguro)
            # Adiciona a coluna como NULL com o tipo correto
            from pyspark.sql.functions import lit
            df_temp = df_temp.withColumn(col_name, lit(None).cast(target_type))

    # Remove colunas que podem estar no arquivo e não no schema final (opcional, mas recomendado)
    cols_to_select = [field.name for field in final_schema]
    df_temp = df_temp.select(*cols_to_select)
    df_temp = df_temp.withColumn("type_data", lit("yellow").cast(StringType()))
    all_dfs.append(df_temp)

# Unir todos os DataFrames
if all_dfs:
    df_yellow_final = all_dfs[0]
    for i in range(1, len(all_dfs)):
        # unionByName é crucial aqui para unir por nome, não por ordem
        df_yellow_final = df_yellow_final.unionByName(all_dfs[i])
