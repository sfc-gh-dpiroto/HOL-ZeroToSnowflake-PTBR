/***************************************************************************************************
       

  _        _  _____  _    __  __   ____        _            
 | |      / \|_   _|/ \  |  \/  | | __ ) _   _| |_ ___  ___ 
 | |     / _ \ | | / _ \ | |\/| | |  _ \| | | | __/ _ \/ __|
 | |___ / ___ \| |/ ___ \| |  | | | |_) | |_| | ||  __/\__ \
 |_____/_/   \_\_/_/   \_\_|  |_| |____/ \__, |\__\___||___/
                                         |___/              
     
                         
Quickstart:   Tasty Bytes - Latam Zero to Snowflake - Monitor de Recursos
Version:      v1
Script:       VOHL_LATAM.sql         
Create Date:  2023-05-16
Author:       Carlos Suarez
Copyright(c): 2023 Snowflake Inc. All rights reserved.
*****************************************************************************************************/




/*----------------------------------------------------------------------------------
Quickstart Sessão 3 - Criação de um almacén

Como administrador do Tasty Bytes Snowflake, recebemos a tarefa de obter um
   compreensão dos recursos que o Snowflake fornece para ajudar a garantir
   A governança financeira está pronta antes de começarmos a consultar e analisar os dados.
 
   Vamos começar criando nosso primeiro warehouse.
-------------------------------------------------- ---------------------------------*/

-- Sessão 3: Passo 1 - Contexto 
USE ROLE latam_tasty_admin;



-- Sessão 3: Passo 2 - Criação e Configuração de um Warehouse
CREATE OR REPLACE WAREHOUSE tasty_test_wh WITH
COMMENT = 'test warehouse for tasty bytes'
    WAREHOUSE_TYPE = 'standard'
    WAREHOUSE_SIZE = 'xsmall' 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 2 
    SCALING_POLICY = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = true
    INITIALLY_SUSPENDED = true;
    

/*----------------------------------------------------------------------------------
Seção de início rápido 4 - Criar um monitor de recursos e aplicá-lo ao nosso warehouse

   Com um warehouse pronto para uso, agora vamos aproveitar os monitores de recursos Snowflakes para garantir
   O warehouse tem uma taxa mensal que permitirá aos nossos administradores acompanhar o seu
   créditos consumidos e certifique-se de que ele seja suspenso se você exceder sua cota atribuída.
-------------------------------------------------- ---------------------------------*/

-- Sessão 4: Passo 1: Criação de um monitor de recursos
USE ROLE accountadmin;
CREATE OR REPLACE RESOURCE MONITOR latam_tasty_test_rm
WITH 
    CREDIT_QUOTA = 100 -- 100 créditos
    FREQUENCY = monthly -- reuniciar métrica mensalmente
    START_TIMESTAMP = immediately -- Inicio Imediato
    TRIGGERS 
        ON 75 PERCENT DO NOTIFY -- Notificar admin da conta aos 75%
        ON 100 PERCENT DO SUSPEND -- suspender WH aos 100%, mas deiar que contulas sejam finalizadas
        ON 110 PERCENT DO SUSPEND_IMMEDIATE; -- Suspender WH e todas consultas em 110%


-- Sessão 4: Passo 2 - LInk entre o monitor de recursos e o Warehouse
ALTER WAREHOUSE tasty_test_wh SET RESOURCE_MONITOR = latam_tasty_test_rm;


/*----------------------------------------------------------------------------------
Sessão de início rápido 5 - Protegendo nosso warehouse de consultas de longo prazo

   Com o monitoramento instalado, agora vamos nos proteger dos bandidos,
   Consultas de longa duração que garantem que os parâmetros de tempo limite sejam ajustados no warehouse.
-------------------------------------------------- ---------------------------------*/

-- Sessão 5: Passo 1 - possiveis parametro a serem ajustados de Warehouse
SHOW PARAMETERS LIKE '%statement%' IN WAREHOUSE tasty_test_wh;


-- Sessão 5: Passo 2 - ajuste do tempo de espera de Whareouse (30min)
ALTER WAREHOUSE tasty_test_wh SET statement_timeout_in_seconds = 1800;


-- Sessão 5: Passo 3 - Ajuste do parâmetro do tempo de fila do Warehouse (10 min)
ALTER WAREHOUSE tasty_test_wh SET statement_queued_timeout_in_seconds = 600;


/*----------------------------------------------------------------------------------
Sessão de início rápido 6 - Protegendo nossa conta de consultoria de longo prazo

   Esses parâmetros de tempo limite também estão disponíveis no nível de conta, usuário e sessão.
   Como não esperamos consultas extremamente longas, vamos agrupar essas
   parâmetros em nossa conta.
 
   No futuro, planejamos monitorá-los como nossas cargas de trabalho do Snowflake
   
-------------------------------------------------- ---------------------------------*/

-- Sessão 6: Passo 1 - Ajuste do parametro de tempo de espera a nivel de conta
ALTER ACCOUNT SET statement_timeout_in_seconds = 18000; 


-- Sessão 6: Passo 2 - Ajuste do parametro de tempo de espera em fila a nível de conta
ALTER ACCOUNT SET statement_queued_timeout_in_seconds = 3600; 


/*----------------------------------------------------------------------------------
Seção de início rápido 7 - Aproveitando, dimensionando e suspendendo nosso warehouse

   Com os blocos de construção da governança financeira implantados, vamos agora aproveitar o Snowflake
   warehouse que criamos para executar consultas. No caminho, escalamos este wahreouse
   para cima e para baixo, além de experimentar a suspensão manual.
-------------------------------------------------- ---------------------------------*/

-- Sessão 7: Passo 1: use nosso Warehouse para executar uma consulta simples
USE ROLE latam_tasty_admin;
USE WAREHOUSE tasty_test_wh; 

    --> atigos de menu vendidos pela loja Cheeky Greek
SELECT 
    m.menu_type,
    m.truck_brand_name,
    m.menu_item_id,
    m.menu_item_name
FROM latam_frostbyte_tasty_bytes.raw_pos.menu m
WHERE truck_brand_name = 'Cheeky Greek';


-- Sessão 7: Passo 2 - escalar nosso warehouse
ALTER WAREHOUSE tasty_test_wh SET warehouse_size = 'XLarge';


-- Sessão 7: Etapa 3: executar uma consulta de agregação em um grande conjunto de dados 
--> calcular o total de pedidos e vendas para nossos membros de fidelidade do cliente 
-->(693M orders / 222K fidelidade)
    
SELECT 
    o.customer_id,
    CONCAT(clm.first_name, ' ', clm.last_name) AS name,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.price) AS total_sales
FROM latam_frostbyte_tasty_bytes.analytics.orders_v o
JOIN latam_frostbyte_tasty_bytes.analytics.customer_loyalty_metrics_v clm
    ON o.customer_id = clm.customer_id
GROUP BY o.customer_id, name
ORDER BY order_count DESC;



-- Sessão 7: Passo 4 - Reduzir capacidade computacional do WH
ALTER WAREHOUSE tasty_test_wh SET warehouse_size = 'XSmall';


-- Sessão 7: Passo 5 - Suspender  Warehouse
ALTER WAREHOUSE tasty_test_wh SUSPEND;
   
/*--
    "Estado não válido. O Warehose não pode ser suspenso". - AUTO_SUSPEND = 60 já ativdo
--*/



/**************************************************************************/
/*------               Quickstart Reset Scripts                     ------*/
/**************************************************** *********************/

USE ROLE accountadmin;
ALTER ACCOUNT SET statement_timeout_in_seconds = default;
ALTER ACCOUNT SET statement_queued_timeout_in_seconds = default; 
DROP WAREHOUSE IF EXISTS tasty_test_wh;
DROP RESOURCE MONITOR IF EXISTS latam_tasty_test_rm; 