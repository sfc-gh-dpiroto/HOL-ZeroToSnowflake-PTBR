/***************************************************************************************************
       

  _        _  _____  _    __  __   ____        _            
 | |      / \|_   _|/ \  |  \/  | | __ ) _   _| |_ ___  ___ 
 | |     / _ \ | | / _ \ | |\/| | |  _ \| | | | __/ _ \/ __|
 | |___ / ___ \| |/ ___ \| |  | | | |_) | |_| | ||  __/\__ \
 |_____/_/   \_\_/_/   \_\_|  |_| |____/ \__, |\__\___||___/
                                         |___/              
     
                         
Quickstart:   Tasty Bytes - Latam Zero to Snowflake - Colaboración
Version:      v1
Script:       VOHL_LATAM.sql         
Create Date:  2023-05-16
Author:       Carlos Suarez
Copyright(c): 2023 Snowflake Inc. All rights reserved.
*****************************************************************************************************/



-- Sessão 3: Passo 1: consultar dados dos pontos de venda
USE ROLE latam_tasty_data_engineer;
USE WAREHOUSE latam_build_wh;

alter warehouse latam_build_wh set warehouse_size = 'xsmall' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;


SELECT 
    o.date,
    SUM(o.price) AS daily_sales
FROM latam_frostbyte_tasty_bytes.analytics.orders_v o
WHERE 1=1
    AND o.country = 'Germany'
    AND o.primary_city = 'Hamburg'
    AND DATE(o.order_ts) BETWEEN '2022-02-10' AND '2022-02-28'
GROUP BY o.date
ORDER BY o.date ASC;


/*------------------------------------------------ ----------------------------------
Seção QuickStart 4: Investigação  dias com zero vendas em nossos próprios dados
   
   Pelo que vimos acima, parece que estamos perdendo as vendas de 16 de fevereiro
   até 21 de fevereiro para Hamburgo. Dentro de nossos próprios dados não há
   muito mais podemos usar para investigar isso, mas algo maior deve ter sido
   em jogo aqui.
 
   Podemos expandir o escopo dos nossos dados para uma visão que seja enriquecida por dados 
   relacionados a clima, que são disponibilizados no Marketplace da Snowflake.
-------------------------------------------------- ---------------------------------*/

-- Sessão 4: Passo 1 - Adicionar Weather Source LLC: disponíveis no Marketplace

/*---
     1. Click em-> Ícone de inicio
     2. Click em -> Marketplace
     3. Buscar -> Frostbyte
     4. Click em -> Weather Source LLC: frostbyte
     5. Click em -> Get / Obeter
     6. Mude o nome da base de dados -> FROSTBYTE_WEATHERSOURCE (caixa alta)
     7. Linkar a role public 
---*/


-- Sessão 4: Passo 2 - Camada Harmonizada com dados próprios e de terceiros
CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.harmonized.daily_weather_v
    AS
SELECT 
    hd.*,
    TO_VARCHAR(hd.date_valid_std, 'YYYY-MM') AS yyyy_mm,
    pc.city_name AS city,
    c.country AS country_desc
FROM frostbyte_weathersource.onpoint_id.history_day hd
JOIN frostbyte_weathersource.onpoint_id.postal_codes pc
    ON pc.postal_code = hd.postal_code
    AND pc.country = hd.country
JOIN latam_frostbyte_tasty_bytes.raw_pos.country c
    ON c.iso_country = hd.country
    AND c.city = hd.city_name;


-- Sessão 4: Passo 3 - Visualização de temperaturas dirárias
SELECT 
    dw.country_desc,
    dw.city_name,
    dw.date_valid_std,
    AVG(dw.avg_temperature_air_2m_f) AS avg_temperature_air_2m_f
FROM latam_frostbyte_tasty_bytes.harmonized.daily_weather_v dw
WHERE 1=1
    AND dw.country_desc = 'Germany'
    AND dw.city_name = 'Hamburg'
    AND YEAR(date_valid_std) = '2022'
    AND MONTH(date_valid_std) = '2'
GROUP BY dw.country_desc, dw.city_name, dw.date_valid_std
ORDER BY dw.date_valid_std DESC;


-- Sessão 4: Passo 4 - Métrivas e Vento e Chuva
SELECT 
    dw.country_desc,
    dw.city_name,
    dw.date_valid_std,
    MAX(dw.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM latam_frostbyte_tasty_bytes.harmonized.daily_weather_v dw
WHERE 1=1
    AND dw.country_desc IN ('Germany')
    AND dw.city_name = 'Hamburg'
    AND YEAR(date_valid_std) = '2022'
    AND MONTH(date_valid_std) = '2'
GROUP BY dw.country_desc, dw.city_name, dw.date_valid_std
ORDER BY dw.date_valid_std DESC;


/*----------------------------------------------------------------------------------
Seção de início rápido 5: Democratização dos insights de dados
 
    Agora determinamos que os ventos do nível do furacão provavelmente estavam ocorrendo para o
    dias com vendas zero que nossos analistas financeiros nos apontaram.

    Agora vamos disponibilizar esse tipo de pesquisa para qualquer pessoa em nossa organização.
    implementando uma visualização do Analytics acessível a todos os funcionários da Tasty Bytes.
-------------------------------------------------- ---------------------------------*/

-- Sessão 5: Passo 1 - Criação de Func SQL
     --> Converte Fahrenheit em Celsius
CREATE OR REPLACE FUNCTION latam_frostbyte_tasty_bytes.analytics.fahrenheit_to_celsius(temp_f NUMBER(35,4))
RETURNS NUMBER(35,4)
AS
$$
    (temp_f - 32) * (5/9)
$$;

    --> converte polegadas para milímetros
CREATE OR REPLACE FUNCTION latam_frostbyte_tasty_bytes.analytics.inch_to_millimeter(inch NUMBER(35,4))
RETURNS NUMBER(35,4)
    AS
$$
    inch * 25.4
$$;

-- Sessão 5: Passo 2 - usando as funcs via SQL
SELECT 
    fd.date_valid_std AS date,
    fd.city_name,
    fd.country_desc,
    ZEROIFNULL(SUM(odv.price)) AS daily_sales,
    ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,
ROUND(AVG(latam_frostbyte_tasty_bytes.analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
    ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
    ROUND(AVG(latam_frostbyte_tasty_bytes.analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
    MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM latam_frostbyte_tasty_bytes.harmonized.daily_weather_v fd
LEFT JOIN latam_frostbyte_tasty_bytes.harmonized.orders_v odv
    ON fd.date_valid_std = DATE(odv.order_ts)
    AND fd.city_name = odv.primary_city
    AND fd.country_desc = odv.country
WHERE 1=1
    AND fd.country_desc = 'Germany'
    AND fd.city = 'Hamburg'
    AND fd.yyyy_mm = '2022-02'
GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc
ORDER BY fd.date_valid_std ASC;


-- Sessão 5: Passo 3: Implementação da View de análise
CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.analytics.daily_city_metrics_v
COMMENT = 'Daily Weather Source Metrics and Orders Data for our Cities'
    AS
SELECT 
    fd.date_valid_std AS date,
    fd.city_name,
    fd.country_desc,
    ZEROIFNULL(SUM(odv.price)) AS daily_sales,
    ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,    ROUND(AVG(latam_frostbyte_tasty_bytes.analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
    ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
    ROUND(AVG(latam_frostbyte_tasty_bytes.analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
    MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM latam_frostbyte_tasty_bytes.harmonized.daily_weather_v fd
LEFT JOIN latam_frostbyte_tasty_bytes.harmonized.orders_v odv
    ON fd.date_valid_std = DATE(odv.order_ts)
    AND fd.city_name = odv.primary_city
    AND fd.country_desc = odv.country
GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc;



/*----------------------------------------------------------------------------------
Sessão de início rápido 6: Obtendo insights de dados de vendas e clima do mercado
 
   Com dados de vendas e clima disponíveis para todas as cidades em que nossos food trucks operam,
   Agora vamos dar uma olhada no valor que fornecemos aos nossos analistas financeiros.
-------------------------------------------------- ---------------------------------*/

-- Sessão 6: Passo 1 - Simplificando nossa Análise (graph)
SELECT 
    dcm.date,
    dcm.city_name,
    dcm.country_desc,
    dcm.daily_sales,
   -- dcm.avg_temperature_fahrenheit,
    dcm.avg_temperature_celsius,
   -- dcm.avg_precipitation_inches,
    dcm.avg_precipitation_millimeters,
    dcm.max_wind_speed_100m_mph
FROM latam_frostbyte_tasty_bytes.analytics.daily_city_metrics_v dcm
WHERE 1=1
    AND dcm.country_desc = 'Germany'
    AND dcm.city_name = 'Hamburg'
    AND dcm.date BETWEEN '2022-02-01' AND '2022-02-24'
ORDER BY date DESC;




/**************************************************************************/
/*------               Quickstart Reset Scripts                     ------*/
/**************************************************** *********************/

USE ROLE accountadmin;

DROP VIEW IF EXISTS latam_frostbyte_tasty_bytes.harmonized.daily_weather_v;
DROP VIEW IF EXISTS latam_frostbyte_tasty_bytes.analytics.daily_city_metrics_v;

DROP DATABASE IF EXISTS frostbyte_weathersource;

DROP FUNCTION IF EXISTS latam_frostbyte_tasty_bytes.analytics.fahrenheit_to_celsius(NUMBER(35,4));
DROP FUNCTION IF EXISTS latam_frostbyte_tasty_bytes.analytics.inch_to_millimeter(NUMBER(35,4));