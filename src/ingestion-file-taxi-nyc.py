# Importa bibliotecas necessárias
import requests
import os

# --- Configurações ---
CATALOG = "ifood_test"
SCHEMA = "default"
VOLUME_NAME = "nyc_taxi_data"

# URL base que hospeda os arquivos (CDN da NYC TLC)
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

# Tipos de arquivos a serem baixados
FILE_TYPES = {
    "yellow": "Yellow Taxi Trip Records",
    "green": "Green Taxi Trip Records"
}

# Ano e meses desejados
YEAR = "2023"
MONTHS = ["01", "02", "03"]  # Janeiro, Fevereiro, Março

# Caminho de destino no Unity Catalog Volume
UC_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/{YEAR}/"

# Cria o diretório de destino, se não existir
try:
    dbutils.fs.mkdirs(UC_PATH)
except Exception:
    pass  # Ignora erro se o diretório já existir

print(f"Iniciando o download dos arquivos para: {UC_PATH}")

# --- Loop de Download ---
download_count = 0
total_files = len(FILE_TYPES) * len(MONTHS)

for month in MONTHS:
    for prefix, description in FILE_TYPES.items():

        # 1. Constrói o nome do arquivo e a URL
        file_name = f"{prefix}_tripdata_{YEAR}-{month}.parquet"
        download_url = f"{BASE_URL}{file_name}"
        local_file_path = os.path.join(UC_PATH, file_name)

        print(f"\nBaixando {description} ({YEAR}-{month})...")
        print(f"URL: {download_url}")

        try:
            # 2. Faz a requisição HTTP com stream para arquivos grandes
            response = requests.get(download_url, stream=True, timeout=300)
            response.raise_for_status()  # Erro para status 4xx ou 5xx

            # 3. Salva o arquivo no Volume
            with open(local_file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            download_count += 1
            print(f"Sucesso! Arquivo salvo em: {local_file_path}")

        except requests.exceptions.HTTPError as e:
            print(f"ERRO HTTP ao baixar {file_name}: {e}")
            print("Verifique se o nome do arquivo ou a URL estão corretos.")
        except Exception as e:
            print(f"ERRO INESPERADO ao baixar {file_name}: {e}")

print(f"\n--- Concluído ---")
print(f"Total de arquivos tentados: {total_files}")
print(f"Total de arquivos baixados com sucesso: {download_count}")

print(f"Os arquivos foram salvos no caminho do Volume: {UC_PATH}")

print("\nArquivos salvos (caminho Volume):")
display(dbutils.fs.ls(UC_PATH))
