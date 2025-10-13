#************************* Qual a média de valor total (total_amount) recebido em um mês considerando todos os yellow táxis da frota? *****************************************#
SELECT
  trip_year,
  trip_month,
  ROUND(AVG(total_amount), 2) AS average_total_amount_monthly, -- Calcula a média e arredonda
  COUNT(*) AS total_trips_in_month
FROM
  ifood_test.default.taxi_trip_yellow -- Use o nome completo da sua tabela
GROUP BY
  trip_year,
  trip_month -- Agrupa pelas colunas de partição
ORDER BY
  trip_year,
  trip_month;

#************************* Qual a média de passageiros (passenger_count) por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?*********#
%sql
WITH CombinedTrips AS (
  -- 1. Seleciona e padroniza os dados do táxi Yellow
  SELECT
    FROM_UTC_TIMESTAMP(tpep_pickup_datetime, 'America/New_York') AS pickup_datetime_ny,
    passenger_count,
    trip_year,
    trip_month
  FROM
    ifood_test.default.taxi_trip_yellow
  WHERE
    trip_month = 5 -- Filtra para o mês de MAIO (Mês 5)
    
  
  UNION ALL
  
  -- 2. Seleciona e padroniza os dados do táxi Green
  SELECT
    FROM_UTC_TIMESTAMP(lpep_pickup_datetime, 'America/New_York') AS pickup_datetime_ny,
    passenger_count,
    trip_year,
    trip_month
  FROM
    ifood_test.default.taxi_trip_green
  WHERE
    trip_month = 5 -- Filtra para o mês de MAIO (Mês 5)
    
)
-- 3. Agrega os resultados combinados pela HORA do dia
SELECT
  HOUR(pickup_datetime_ny) AS pickup_hour_of_day, -- Extrai a hora do dia (0-23)
  ROUND(AVG(passenger_count), 2) AS average_passengers_per_hour,
  COUNT(*) AS total_trips_in_hour
FROM
  CombinedTrips
GROUP BY
  pickup_hour_of_day
ORDER BY
  pickup_hour_of_day;
