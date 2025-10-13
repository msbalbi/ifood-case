# Importa bibliotecas necessárias
import requests
import os
import logging

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Widgets para parametrização ---
dbutils.widgets.text("CATALOG", "ifood_test", "Nome do Catalog")
dbutils.widgets.text("SCHEMA", "default", "Nome do Schema")
dbutils.widgets.text("VOLUME_NAME", "nyc_taxi_data", "Nome do Volume")
dbutils.widgets.text("YEAR", "2023", "Ano dos dados")
dbutils.widgets.text("MONTHS", "01,02,03", "Meses separados por vírgula")
dbutils.widgets.text("FILE_TYPES", "yellow,green", "Tipos de arquivos separados por vírgula")

# --- Recupera parâmetros ---
CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
VOLUME_NAME = dbutils.widgets.get("VOLUME_NAME")
YEAR = dbutils.widgets.get("YEAR")
MONTHS = dbutils.widgets.get("MONTHS").split(",")
FILE_TYPES_INPUT = dbutils.widgets.get("FILE_TYPES").split(",")

# Mapeamento de tipos de arquivo para descrição
FILE_TYPES = {
    "yellow": "Yellow Taxi Trip Records",
    "green": "Green Taxi Trip Records"
}
# Filtra FILE_TYPES com base no input
FILE_TYPES = {k: v for k, v in FILE_TYPES.items() if k in FILE_TYPES_INPUT}

# URL base que hospeda os arquivos (CDN da NYC TLC)
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

# Caminho de destino no Unity Catalog Volume
UC_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/{YEAR}/"

# Cria o diretório de destino, se não existir
try:
    dbutils.fs.mkdirs(UC_PATH)
except Exception:
    pass  # Ignora erro se o diretório já existir

logger.info(f"Iniciando o download dos arquivos para: {UC_PATH}")

# --- Loop de Download ---
download_count = 0
total_files = len(FILE_TYPES) * len(MONTHS)

for month in MONTHS:
    for prefix, description in FILE_TYPES.items():

        # Constrói o nome do arquivo e a URL
        file_name = f"{prefix}_tripdata_{YEAR}-{month}.parquet"
        download_url = f"{BASE_URL}{file_name}"
        local_file_path = os.path.join(UC_PATH, file_name)

        logger.info(f"\nBaixando {description} ({YEAR}-{month})")
        logger.info(f"URL: {download_url}")

        try:
            # Requisição HTTP com stream para arquivos grandes
            response = requests.get(download_url, stream=True, timeout=300)
            response.raise_for_status()  # Erro para status 4xx ou 5xx

            # Salva o arquivo no Volume
            with open(local_file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            download_count += 1
            logger.info(f"Sucesso! Arquivo salvo em: {local_file_path}")

        except requests.exceptions.HTTPError as e:
            logger.error(f"ERRO HTTP ao baixar {file_name}: {e}")
            logger.warning("Verifique se o nome do arquivo ou a URL estão corretos.")
        except Exception as e:
            logger.error(f"ERRO INESPERADO ao baixar {file_name}: {e}")

logger.info(f"\n--- Concluído ---")
logger.info(f"Total de arquivos tentados: {total_files}")
logger.info(f"Total de arquivos baixados com sucesso: {download_count}")
logger.info(f"Os arquivos foram salvos no caminho do Volume: {UC_PATH}")

logger.info("\nArquivos salvos (caminho Volume):")
display(dbutils.fs.ls(UC_PATH))
