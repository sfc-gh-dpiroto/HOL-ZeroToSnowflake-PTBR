/***************************************************************************************************
       

  _        _  _____  _    __  __   ____        _            
 | |      / \|_   _|/ \  |  \/  | | __ ) _   _| |_ ___  ___ 
 | |     / _ \ | | / _ \ | |\/| | |  _ \| | | | __/ _ \/ __|
 | |___ / ___ \| |/ ___ \| |  | | | |_) | |_| | ||  __/\__ \
 |_____/_/   \_\_/_/   \_\_|  |_| |____/ \__, |\__\___||___/
                                         |___/              
     
                         
Quickstart:   Tasty Bytes - Latam Zero to Snowflake - Geoespacial
Version:      v1
Script:       VOHL_LATAM.sql         
Create Date:  2023-05-16
Author:       Carlos Suarez
Copyright(c): 2023 Snowflake Inc. All rights reserved.
*****************************************************************************************************/




/*----------------------------------------------------------------------------------
Seção de início rápido 3: Aquisição de dados Safegraph POI do Snowflake Marketplace

   A Tasty Bytes opera Food Trucks em várias cidades e países ao redor do mundo com cada 
   caminhão tendo a capacidade de escolher dois locais de vendas diferentes
   por dia (AM/PM). Um ponto importante que interessa aos nossos Executivos é aprender
   mais sobre como esses locais se relacionam entre si, bem como se há algum
   locais que atendemos atualmente que estão potencialmente muito longe dos principais vendedores
   centros das cidades.

   Infelizmente, o que vimos até agora é que nossos dados de primeira mão não nos dão
   os componentes básicos necessários para completar este tipo de análise geoespacial.
 
   Felizmente, o Snowflake Marketplace tem ótimas listagens Safegraph que
   você pode nos ajudar aqui.
-------------------------------------------------- ---------------------------------*/

-- Sessão 3: Passo 1: Análise exporatória para encontrar localizações com mais vendas
-- Não queremos que a escolha do lugar seja baseada em uma escolha aleatória
USE ROLE latam_tasty_data_engineer;

USE WAREHOUSE latam_build_wh;

SELECT TOP 10
    o.location_id,
    SUM(o.price) AS total_sales_usd
FROM latam_frostbyte_tasty_bytes.analytics.orders_v o
WHERE 1=1
    AND o.primary_city = 'Paris'
    AND YEAR(o.date) = 2022
GROUP BY o.location_id
ORDER BY total_sales_usd DESC;


-- Section 3:Passo 2: Adicionar POI (Points of interest) de Safegraph no Snowflake Marketplace
/*--
     - Click em -> Icone de início
     - Click en -> marketplace
     - Buscar -> frostbyte
     - Click em -> SafeGraph: frostbyte
     - Click em -> get / obter
     - Mudar o nome da base de dados -> FROSTBYTE_SAFEGRAPH (Caixa Alta)
     - Role Public
--*/


-- Sessão 3: Passo 3 - Avaliação de dados POI de Safegraph
-- Muitas métricas de Ponto de Interesse (Lat, long, Endereço, Nome do Local etc)
SELECT 
    cpg.location_id,
    cpg.placekey,
    cpg.location_name,
    cpg.longitude,
    cpg.latitude,
    cpg.street_address,
    cpg.city,
    cpg.country,
    cpg.polygon_wkt
FROM frostbyte_safegraph.public.frostbyte_tb_safegraph_s cpg
WHERE 1=1
    AND cpg.top_category = 'Museums, Historical Sites, and Similar Institutions'
    AND cpg.sub_category = 'Museums'
    AND cpg.city = 'Paris'
    AND cpg.country = 'France';


/*----------------------------------------------------------------------------------
Quickstart Seção 4 - Harmonização e promoção de dados próprios e de terceiros

   Para que nossa análise geoespacial seja perfeita, certifique-se de obter o Safegraph POI
   dados incluídos em analytics.orders_v para que todos os nossos usuários downstream possam
   também acesse.
-------------------------------------------------- ---------------------------------*/

-- Sessão 4: Passo 1 - Enriquecer nossa view de analise
USE ROLE sysadmin;

CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.analytics.orders_v
COMMENT = 'Tasty Bytes Order Detail View'
    AS
SELECT 
    DATE(o.order_ts) AS date,
    o.* ,
    cpg.* EXCLUDE (location_id, region, phone_number, country)
FROM latam_frostbyte_tasty_bytes.harmonized.orders_v o
JOIN frostbyte_safegraph.public.frostbyte_tb_safegraph_s cpg
    ON o.location_id = cpg.location_id;


/*----------------------------------------------------------------------------------
Sessão de início rápido 5 - Realizando análise geoespacial - Parte 1

   Com métricas de POI (point of interest) agora disponíveis no Snowflake Marketplace
   sem a necessidade de ETL, nosso engenheiro de dados Tasty Bytes agora pode começar a
   análise geoespacial.
-------------------------------------------------- ---------------------------------*/    

-- Sessão 5: Passo 1: Criação de um ponto geográfico (10 mais reevantes)
USE ROLE latam_tasty_data_engineer;

SELECT TOP 10 
     o.location_id,
     o.location_name,
     SUM(o.price) AS total_sales_usd,
     ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point
FROM latam_frostbyte_tasty_bytes.analytics.orders_v o
WHERE 1=1
    AND o.primary_city = 'Paris'
    AND YEAR(o.date) = 2022
GROUP BY o.location_id,o.location_name,o.latitude, o.longitude
ORDER BY total_sales_usd DESC;




-- Sessão 5: Passo 2 - Calculo de distância entre localizações através da função ST_Distance (geospacial distance)
-- "Qualify" window function evita duplicidades em comparações
WITH _top_10_locations AS 
(
    SELECT TOP 10
        o.location_id,
        ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point,
        SUM(o.price) AS total_sales_usd
    FROM latam_frostbyte_tasty_bytes.analytics.orders_v o
    WHERE 1=1
        AND o.primary_city = 'Paris'
        AND YEAR(o.date) = 2022
    GROUP BY o.location_id, o.latitude, o.longitude
    ORDER BY total_sales_usd DESC
)
SELECT
    a.location_id,
    b.location_id,
    ROUND(ST_DISTANCE(a.geo_point, b.geo_point)/1609,2) AS geography_distance_miles,
    ROUND(ST_DISTANCE(a.geo_point, b.geo_point)/1000,2) AS geography_distance_kilometers
FROM _top_10_locations a  
JOIN _top_10_locations b
    ON a.location_id <> b.location_id -- avoid calculating the distance between the point itself
QUALIFY a.location_id <> LAG(b.location_id) OVER (ORDER BY geography_distance_miles) -- avoid duplicate: a to b, b to a distances
ORDER BY geography_distance_miles;


/*----------------------------------------------------------------------------------
Sessão de início rápido 6 - Realizando análise geoespacial - Parte 1

   Agora que entendemos como criar pontos e calcular distâncias, vamos
   acumule um grande conjunto de recursos geoespaciais adicionais do Snowflake para promover nossa
   análise.
-------------------------------------------------- ---------------------------------*/

-- Sessão 6: Passo 1 - Coleta de pontos, desenho de um polígono delimitador mínimo e cálculo da área
--coletamos os pontos, desenhamos um "minimum_bouding_polygon" ao redor de cada localização e, por fim, calculamos a área
WITH _top_10_locations AS 
(
    SELECT TOP 10
        o.location_id,
        ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point,
        SUM(o.price) AS total_sales_usd
    FROM latam_frostbyte_tasty_bytes.analytics.orders_v o
    WHERE 1=1
        AND o.primary_city = 'Paris'
        AND YEAR(o.date) = 2022
    GROUP BY o.location_id, o.latitude, o.longitude
    ORDER BY total_sales_usd DESC
)
SELECT
    ST_NPOINTS(ST_COLLECT(tl.geo_point)) AS count_points_in_collection,
    ST_COLLECT(tl.geo_point) AS collection_of_points,
    ST_ENVELOPE(collection_of_points) AS minimum_bounding_polygon,
    ROUND(ST_AREA(minimum_bounding_polygon)/1000000,2) AS area_in_sq_kilometers
FROM _top_10_locations tl;


-- Sessão 6: Passo 2:Encontrar locais de maior fluxo de vendas
--Fução CENTROID retorna a localização central entre diferentes objetos geograficos ou geometricos
WITH _top_10_locations AS 
(
    SELECT TOP 10
        o.location_id,
        ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point,
        SUM(o.price) AS total_sales_usd
    FROM latam_frostbyte_tasty_bytes.analytics.orders_v o
    WHERE 1=1
        AND o.primary_city = 'Paris'
        AND YEAR(o.date) = 2022
    GROUP BY o.location_id, o.latitude, o.longitude
    ORDER BY total_sales_usd DESC
)
SELECT  
    ST_COLLECT(tl.geo_point) AS collect_points,
    ST_CENTROID(collect_points) AS geometric_center_point
FROM _top_10_locations tl;


-- Sessão 6: Passo 3 - Definir uma variabel SQL como ponto de venda central para calculo das distancias
SET center_point = '{   "coordinates": [     2.364853294993676e+00,     4.885681511418426e+01   ],   "type": "Point" }';


-- Sessão 6: Passo 4: encontre os locais mais distantes do nosso ponto central de vendas
-- Insights de localizações para evitar, já que sao os locais com menos vendas.
WITH _2022_paris_locations AS
(
    SELECT DISTINCT 
        o.location_id,
        o.location_name,
        ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point
    FROM latam_frostbyte_tasty_bytes.analytics.orders_v o
    WHERE 1=1
        AND o.primary_city = 'Paris'
        AND YEAR(o.date) = 2022
)
SELECT TOP 50
    ll.location_id,
    ll.location_name,
    ROUND(ST_DISTANCE(ll.geo_point, TO_GEOGRAPHY($center_point))/1000,2) AS kilometer_from_top_selling_center
FROM _2022_paris_locations ll
ORDER BY kilometer_from_top_selling_center DESC;

--Somente parte do que é possível ser feito com dados geoespaciais.


/**************************************************************************/
/*------               Quickstart Reset Scripts                     ------*/
/**************************************************** *********************/

UNSET center_point;

USE ROLE sysadmin;

CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.analytics.orders_v
COMMENT = 'Tasty Bytes Order Detail View'
    AS
SELECT DATE(o.order_ts) AS date, * FROM latam_frostbyte_tasty_bytes.harmonized.orders_v o;

USE ROLE ACCOUNTADMIN;
DROP DATABASE IF EXISTS frostbyte_safegraph;