README_ifood_case_fixed.md# 🚖 Case Técnico — Data Architect | iFood  
### **Ingestão e Análise de Dados de Táxis de Nova York**

---

## 🎯 **Objetivo**

Esta solução demonstra a construção de um **pipeline de ingestão, processamento e análise** para os dados públicos de corridas de táxi de Nova York (Yellow e Green Taxis), referente ao período **janeiro a maio de 2023**, aplicando o padrão **Lakehouse Architecture** no ambiente **Databricks / Unity Catalog**.

---

## 🧩 **Arquitetura Geral da Solução**

Abaixo um diagrama simplificado (em texto) da arquitetura para garantir compatibilidade com a visualização do GitHub:

```
[Download de Dados Públicos] --> [Volume no Unity Catalog] --> [Processamento e Padronização - PySpark]
      --> [Tabelas Delta Lake (Governadas)] --> [Consultas SQL Analíticas]
```

---

## 1️⃣ **Ingestão e Armazenamento**  
📂 Arquivo: `src/ingestion-file-taxi-nyc.py`

### 🧭 **Resumo Geral**

O script automatiza o **download dos arquivos públicos de viagens de táxi de Nova York** diretamente do **CDN oficial da NYC TLC (Taxi and Limousine Commission)** e os armazena em um **Volume do Unity Catalog** no Databricks.

> 💡 Foi desenvolvido para ser executado **dentro de um notebook Databricks**, pois utiliza `dbutils.fs` e caminhos `/Volumes/...`.

---

### ⚙️ **Etapas Principais**

#### 1. Importação e Configuração
- Bibliotecas: `requests`, `os`, `dbutils`
- Configurações:
  - `CATALOG`, `SCHEMA`, `VOLUME_NAME`: destino no Unity Catalog  
  - `BASE_URL`: origem dos arquivos  
  - `FILE_TYPES`: tipos de táxi (`yellow`, `green`)  
  - `YEAR` e `MONTHS`: período de ingestão (ex: jan–mai/2023)

#### 2. Criação de Diretório
Cria o diretório no volume com:
```python
dbutils.fs.mkdirs(UC_PATH)
```
Ignora erro se já existir.

#### 3. Download dos Arquivos
Loop duplo sobre meses e tipos (`yellow`, `green`):

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
```

- Faz o download com `requests.get(..., stream=True)`
- Salva o arquivo diretamente no **Volume do Unity Catalog**

#### 4. Tratamento de Erros
- Captura exceções HTTP e timeout
- Exibe logs amigáveis no notebook

#### 5. Relatório Final
- Exibe total de arquivos baixados
- Lista o conteúdo com:
```python
display(dbutils.fs.ls(UC_PATH))
```

---

### 🧩 **Resumo Funcional**

| Função | Descrição |
|--------|------------|
| `requests.get()` | Baixa os arquivos .parquet |
| `dbutils.fs.mkdirs()` | Cria diretório no volume |
| `dbutils.fs.ls()` | Lista arquivos armazenados |
| `open(..., 'wb')` | Salva o conteúdo binário |

---

### 💡 **Como Executar**

1. Rodar o script em um **notebook Databricks (Python)**.  
2. Ajustar:
   - `CATALOG`, `SCHEMA`, `VOLUME_NAME`
   - `YEAR`, `MONTHS`
3. Executar — o processo fará o download e salvará automaticamente os arquivos `.parquet`.

---

## 2️⃣ **Transformação e Carga (ETL)**  
📂 Arquivo: `src/ingestion-file-taxi-nyc.py`

### 🧩 **Resumo — Processamento e Carga dos Dados NYC Taxi (Yellow e Green)**

Este módulo representa a camada de **Transformação + Load** do pipeline.  
Ele lê os arquivos Parquet, padroniza schemas, unifica datasets e grava tabelas **Delta** no Unity Catalog.

---

### ⚙️ **Etapas Principais**

#### 1. Leitura dos Arquivos
Leitura via `spark.read.parquet()` dos arquivos:
```
/Volumes/ifood_test/default/nyc_taxi_data/2023/
```

- **Yellow Taxi** → `yellow_tripdata_2023-*.parquet`
- **Green Taxi** → `green_tripdata_2023-*.parquet`

#### 2. Padronização de Schema
Define schemas consistentes via `StructType`:
- `LongType` → IDs
- `DoubleType` → valores
- `TimestampType` → datas
- `StringType` → flags

Adiciona coluna `type_data` (“yellow” ou “green”).

#### 3. União e Casting
- Alinha colunas via `unionByName`
- Cria colunas ausentes como `NULL`
- Converte tipos com `withColumn().cast()`

#### 4. Enriquecimento
Adiciona:
```python
trip_year, trip_month = year(pickup_datetime), month(pickup_datetime)
```

#### 5. Escrita no Delta Lake
Salva tabelas finais:

| Tabela | Partições |
|---------|-----------|
| `ifood_test.default.taxi_trip_yellow` | `trip_year`, `trip_month` |
| `ifood_test.default.taxi_trip_green` | `trip_year`, `trip_month` |

Usa:
```python
.option("replaceWhere", "trip_year = 2023 AND trip_month IN (1,2,3)")
```
para reprocessamento seletivo.

---

### 🧠 **Principais Recursos Técnicos**

| Recurso | Descrição |
|----------|------------|
| **PySpark DataFrame API** | Transformação eficiente de grandes volumes |
| **Schema enforcement** | Evita inconsistências entre meses |
| **Delta Lake** | Garantia ACID e versionamento |
| **Partitioning** | Acelera consultas |
| **replaceWhere** | Substituição seletiva por partição |

---

### 🧾 **Resumo Final**

Camada **T+L (Transform + Load)** responsável por:
- Ler dados brutos dos volumes
- Padronizar e corrigir schemas
- Unificar dados mensais
- Persistir em **Delta Tables governadas**

---

## 3️⃣ **Análise e Consumo (SQL Analytics)**  
📂 Arquivo: `src/analysis_taxi_nyc.sql`

### 📊 **Consultas Analíticas**

Conjunto de queries SQL em Databricks que respondem a perguntas de negócio com base nas tabelas Delta Lake do Unity Catalog.

---

### 🧮 **1. Média mensal do valor total recebido (Yellow Taxi)**

**Objetivo:** Calcular a média do valor total (`total_amount`) recebido por mês.  
**Fonte:** `ifood_test.default.taxi_trip_yellow`

```sql
SELECT
  trip_year,
  trip_month,
  ROUND(AVG(total_amount), 2) AS average_total_amount_monthly,
  COUNT(*) AS total_trips_in_month
FROM ifood_test.default.taxi_trip_yellow
GROUP BY trip_year, trip_month
ORDER BY trip_year, trip_month;
```

🧾 **Resultado esperado:**  
Tendência mensal de faturamento médio por viagem (sazonalidade e receita).

---

### 👥 **2. Média de passageiros por hora do dia (Yellow + Green)**

**Objetivo:** Identificar o comportamento de demanda por hora do dia (mês de maio).  
**Fontes:** `taxi_trip_yellow` + `taxi_trip_green`

```sql
WITH CombinedTrips AS (
  SELECT FROM_UTC_TIMESTAMP(tpep_pickup_datetime, 'America/New_York') AS pickup_datetime_ny,
         passenger_count
  FROM ifood_test.default.taxi_trip_yellow
  WHERE trip_month = 5
  UNION ALL
  SELECT FROM_UTC_TIMESTAMP(lpep_pickup_datetime, 'America/New_York') AS pickup_datetime_ny,
         passenger_count
  FROM ifood_test.default.taxi_trip_green
  WHERE trip_month = 5
)
SELECT
  HOUR(pickup_datetime_ny) AS pickup_hour_of_day,
  ROUND(AVG(passenger_count), 2) AS average_passengers_per_hour,
  COUNT(*) AS total_trips_in_hour
FROM CombinedTrips
GROUP BY pickup_hour_of_day
ORDER BY pickup_hour_of_day;
```

🧾 **Resultado esperado:**  
Perfil horário da demanda (picos, horários de rush, padrões de movimentação).

---

### 🧠 **Insights e Aplicações**

| Métrica | Objetivo de Negócio |
|----------|--------------------|
| **Média mensal de total_amount** | Avaliar sazonalidade e receita média |
| **Média de passageiros/hora** | Otimizar alocação de frota e identificar horários de pico |

---

### 💾 **Tabelas Utilizadas**

- `ifood_test.default.taxi_trip_yellow`  
- `ifood_test.default.taxi_trip_green`

---

### 🧩 **Ferramentas**

- Databricks SQL / Spark SQL  
- Delta Lake  
- Funções: `AVG`, `COUNT`, `HOUR`, `ROUND`, `FROM_UTC_TIMESTAMP`, `UNION ALL`

---

## 🏁 **Conclusão**

Este projeto demonstra o ciclo completo de um pipeline **Lakehouse** em Databricks:
1. **Ingestão** automatizada de dados públicos.  
2. **Transformação** e padronização em Delta Lake.  
3. **Análise SQL** integrada e escalável.

📘 **Stack Utilizada**
- Databricks
- PySpark
- Delta Lake
- Unity Catalog
- Spark SQL

---

👤 **Autor:** *Márcio Serafim*  
📅 *Outubro / 2025*  
🏢 *Case Técnico — iFood Data Architect*
