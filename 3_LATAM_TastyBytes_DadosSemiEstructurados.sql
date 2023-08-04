/***************************************************************************************************
       

  _        _  _____  _    __  __   ____        _            
 | |      / \|_   _|/ \  |  \/  | | __ ) _   _| |_ ___  ___ 
 | |     / _ \ | | / _ \ | |\/| | |  _ \| | | | __/ _ \/ __|
 | |___ / ___ \| |/ ___ \| |  | | | |_) | |_| | ||  __/\__ \
 |_____/_/   \_\_/_/   \_\_|  |_| |____/ \__, |\__\___||___/
                                         |___/              
     
                         
Quickstart:   Tasty Bytes - Latam-  Zero to Snowflake - Dato Semi-Estructurado
Version:      v1
Script:       VOHL_LATAM.sql         
Create Date:  2023-05-16
Author:       Carlos Suarez
Copyright(c): 2023 Snowflake Inc. All rights reserved.
*****************************************************************************************************/


-- Sessão  3: Passo  1 - Set de contexto
USE ROLE latam_tasty_data_engineer;

USE WAREHOUSE latam_build_wh;


alter warehouse latam_build_wh set warehouse_size = 'xsmall' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- selecionar menú
SELECT TOP 10
    m.truck_brand_name,
    m.menu_type,
    m.menu_item_name,
    m.menu_item_health_metrics_obj
FROM latam_frostbyte_tasty_bytes.raw_pos.menu m;


-- Sessão  3: Passo  2 - Explorar coluna semi-estruturada
SHOW COLUMNS IN latam_frostbyte_tasty_bytes.raw_pos.menu;


-- Sessão  3: Passo  3 - consultar dados semi-estruturados através de "dot notation"
Select
    m.menu_item_health_metrics_obj:menu_item_id AS menu_item_id,
    m.menu_item_health_metrics_obj:menu_item_health_metrics AS menu_item_health_metrics
FROM latam_frostbyte_tasty_bytes.raw_pos.menu m;


/*----------------------------------------------------------------------------------
Sessão de início rápido 4 - Flatten de dados semiestruturados
   Tendo visto como podemos facilmente consultar dados semiestruturados como eles existem em uma variante
   coluna usando notação de ponto, nosso Tasty Data Engineer está no caminho certo para fornecer
   seus stakeholders internos com os dados solicitados.

   Nesta Sessão, realizaremos processamento de dados semiestruturados adicionais.
   para preencher os requerimentos.
-------------------------------------------------- ---------------------------------*/

-- Sessão  4: Passo  1 - introdução Lateral Flatten (acessar objetos dentro de objetos)
SELECT 
    m.menu_item_name,
    obj.value:"ingredients"::VARIANT AS ingredients
FROM latam_frostbyte_tasty_bytes.raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;

    
-- Sessão  4: Passo  2 - Usando uma função de matriz matriz para consultas e extrar ingredientes
-- itens do menu que possuam alface
SELECT 
    m.menu_item_name,
    obj.value:"ingredients"::VARIANT AS ingredients
FROM latam_frostbyte_tasty_bytes.raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj
WHERE ARRAY_CONTAINS('Lettuce'::VARIANT, obj.value:"ingredients"::VARIANT);


-- Sessão  4: Passo  3 -Aplicando a todos os campos 
SELECT 
    m.menu_item_health_metrics_obj:menu_item_id::integer AS menu_item_id,
    m.menu_item_name,
    obj.value:"ingredients"::VARIANT AS ingredients,
    obj.value:"is_healthy_flag"::VARCHAR(1) AS is_healthy_flag,
    obj.value:"is_gluten_free_flag"::VARCHAR(1) AS is_gluten_free_flag,
    obj.value:"is_dairy_free_flag"::VARCHAR(1) AS is_dairy_free_flag,
    obj.value:"is_nut_free_flag"::VARCHAR(1) AS is_nut_free_flag
FROM latam_frostbyte_tasty_bytes.raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;
    

/*----------------------------------------------------------------------------------
Sessão de início rápido 5 - Criando Views estruturadas em dados semiestruturados

   Na última Sessão , construímos uma consulta que fornece o resultado exato para o nosso propósito
   os usuários exigem o uso de um conjunto de recursos de dados semiestruturados Snowflake em conjunto     com o caminho. A seguir seguiremos o processo de promoção desta consulta junto ao nosso Raw
   camada por meio do Harmonized e, eventualmente, para o Analytics, onde nossos usuários finais estão
   privilégio de ler.
-------------------------------------------------- ---------------------------------*/

-- Sessão  5: Passo  1 - Criando view na camada harmonizada com dados Semi-Estructurado
CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.harmonized.menu_v
    AS
SELECT 
    m.menu_id,
    m.menu_type_id,
    m.menu_type,
    m.truck_brand_name,
    m.menu_item_health_metrics_obj:menu_item_id::integer AS menu_item_id,
    m.menu_item_name,
    m.item_category,
    m.item_subcategory,
    m.cost_of_goods_usd,
    m.sale_price_usd,
    obj.value:"ingredients"::VARIANT AS ingredients,
    obj.value:"is_healthy_flag"::VARCHAR(1) AS is_healthy_flag,
    obj.value:"is_gluten_free_flag"::VARCHAR(1) AS is_gluten_free_flag,
    obj.value:"is_dairy_free_flag"::VARCHAR(1) AS is_dairy_free_flag,
    obj.value:"is_nut_free_flag"::VARCHAR(1) AS is_nut_free_flag
FROM latam_frostbyte_tasty_bytes.raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;

    
-- Sessão  5: Passo  2: Promover view harmonizada para camada analitica
CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.analytics.menu_v
COMMENT = 'Menu level metrics including Truck Brands and Menu Item details including cost, price, ingredients and dietary restrictions'
    AS
SELECT 
    * 
    EXCLUDE (menu_type_id) --exclude MENU_TYPE_ID
    RENAME  (truck_brand_name AS brand_name) -- rename TRUCK_BRAND_NAME to BRAND_NAME
FROM latam_frostbyte_tasty_bytes.harmonized.menu_v;


/*----------------------------------------------------------------------------------
Sessão de início rápido 6: análise de dados semiestruturados processados ​​no Snowsight

   Com nossa view de menu disponível em nossa camada de análise, vamos executar algumas consultas
   contra ele que forneceremos aos nossos usuários finais, mostrando como Snowflake fortalece
   uma experiência de consulta relacional em dados semiestruturados sem ter que fazer
   cópias adicionais ou realizar qualquer processamento complexo.
-------------------------------------------------- ---------------------------------*/

-- Sessão  6: Passo  1 - Análise de de Arrays (Array_intersection)
SELECT 
    m1.menu_type,
    m1.menu_item_name,
    m2.menu_type AS overlap_menu_type,
    m2.menu_item_name AS overlap_menu_item_name,
    ARRAY_INTERSECTION(m1.ingredients, m2.ingredients) AS overlapping_ingredients
FROM latam_frostbyte_tasty_bytes.analytics.menu_v m1
JOIN latam_frostbyte_tasty_bytes.analytics.menu_v m2
    ON m1.menu_item_id <> m2.menu_item_id -- evitar join de um item a ele mesmo
    AND m1.menu_type <> m2.menu_type 
WHERE 1=1
    AND m1.item_category <> 'Beverage' -- excluir bebidas
    AND m2.item_category <> 'Beverage' -- excluir bebidas
    AND ARRAYS_OVERLAP(m1.ingredients, m2.ingredients) -- classifica como 'true' se os ingredientes estão em ambas as matrizes
ORDER BY m1.menu_type;


-- Sessão  6: Passo  2 - Extrair métricas de negócio para camada executiva
SELECT
    COUNT(DISTINCT menu_item_id) AS total_menu_items,
    SUM(CASE WHEN is_healthy_flag = 'Y' THEN 1 ELSE 0 END) AS healthy_item_count,
    SUM(CASE WHEN is_gluten_free_flag = 'Y' THEN 1 ELSE 0 END) AS gluten_free_item_count,
    SUM(CASE WHEN is_dairy_free_flag = 'Y' THEN 1 ELSE 0 END) AS dairy_free_item_count,
    SUM(CASE WHEN is_nut_free_flag = 'Y' THEN 1 ELSE 0 END) AS nut_free_item_count
FROM latam_frostbyte_tasty_bytes.analytics.menu_v m;


-- Sessão  6: Passo  3 - visualização gráfica
SELECT
    m.brand_name,
    SUM(CASE WHEN is_gluten_free_flag = 'Y' THEN 1 ELSE 0 END) AS gluten_free_item_count,
    SUM(CASE WHEN is_dairy_free_flag = 'Y' THEN 1 ELSE 0 END) AS dairy_free_item_count,
    SUM(CASE WHEN is_nut_free_flag = 'Y' THEN 1 ELSE 0 END) AS nut_free_item_count
FROM latam_frostbyte_tasty_bytes.analytics.menu_v m
WHERE m.brand_name IN  ('Plant Palace', 'Peking Truck','Revenge of the Curds')
GROUP BY m.brand_name;



/**************************************************************************/
/*------               Quickstart Reset Scripts                     ------*/
/**************************************************** *********************/

DROP VIEW IF EXISTS latam_frostbyte_tasty_bytes.harmonized.menu_v;
DROP VIEW IF EXISTS latam_frostbyte_tasty_bytes.analytics.menu_v;