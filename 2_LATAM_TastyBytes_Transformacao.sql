/***************************************************************************************************
       

  _        _  _____  _    __  __   ____        _            
 | |      / \|_   _|/ \  |  \/  | | __ ) _   _| |_ ___  ___ 
 | |     / _ \ | | / _ \ | |\/| | |  _ \| | | | __/ _ \/ __|
 | |___ / ___ \| |/ ___ \| |  | | | |_) | |_| | ||  __/\__ \
 |_____/_/   \_\_/_/   \_\_|  |_| |____/ \__, |\__\___||___/
                                         |___/              
     
                         
Quickstart:   Tasty Bytes - Latam Zero to Snowflake - Transformación
Version:      v1
Script:       VOHL_LATAM.sql         
Create Date:  2023-05-16
Author:       Carlos Suarez
Copyright(c): 2023 Snowflake Inc. All rights reserved.
*****************************************************************************************************/

--Cenario: ADD nova coluna e corrir Typo de produção.

-- Section 3: Step 1 - Clone do DB de Produção
USE ROLE latam_tasty_dev;

CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.truck_dev 
    CLONE latam_frostbyte_tasty_bytes.raw_pos.truck;
--
--select count(*) from latam_frostbyte_tasty_bytes.raw_pos.order_detail; --673M
--CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.order_detail_dev
  --  CLONE latam_frostbyte_tasty_bytes.raw_pos.order_detail;
--drop table latam_frostbyte_tasty_bytes.raw_pos.order_detail_dev;
      
/*----------------------------------------------------------------------------------
Sessão de inicio rápido 4: Testando o cache do conjunto de resultados da consulta Snowflakes

   Com nosso Zero Copy Clone disponível instantaneamente, agora podemos começar a desenvolver contra
   sem medo de afetar a produção. No entanto, antes de fazer qualquer alteração
   vamos primeiro executar algumas consultas simples e ver como o Cache Snowflake
   é usado.
-------------------------------------------------- ---------------------------------*/

-- Sessão 4: Passo 1 - Consultar tabela clonada
USE WAREHOUSE latam_build_wh;

SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model
FROM latam_frostbyte_tasty_bytes.raw_pos.truck_dev t
ORDER BY t.truck_id;


-- Sessão 4: Passo 2 - nova consulta
SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model
FROM latam_frostbyte_tasty_bytes.raw_pos.truck_dev t
ORDER BY t.truck_id;

    
/*----------------------------------------------------------------------------------
Seção de início rápido 5: atualização de dados e cálculo da idade do Food Truck

   Com base em nossa saída acima, primeiro precisamos corrigir o erro de digitação nesses registros Ford_.
   vimos em nossa coluna `make`. A partir daí, podemos começar a trabalhar em nosso cálculo.
   que nos dará a idade de cada caminhão.
-------------------------------------------------- ---------------------------------*/

-- Sessão 5: Passo 1 - Atualização de valor incorreto
UPDATE latam_frostbyte_tasty_bytes.raw_pos.truck_dev 
SET make = 'Ford' 
WHERE make = 'Ford_';


-- Sessão 5: Passo 2 - Criação do Calculo de Idade
SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model,
    (YEAR(CURRENT_DATE()) - t.year) AS truck_age_year
FROM latam_frostbyte_tasty_bytes.raw_pos.truck_dev t;


/*----------------------------------------------------------------------------------
Seção de início rápido 6: adicionar uma coluna e atualizá-la

   Com o cálculo da idade do caminhão em anos feito e limpo, vamos agora adicionar uma nova coluna
   para a nossa tabela clonada para fazer backup e terminar as coisas atualizando a coluna para
   refletem os valores calculados.
-------------------------------------------------- ---------------------------------*/

-- Sessão 6: Passo 1 - Adicionar nova coluna para Idade
ALTER TABLE latam_frostbyte_tasty_bytes.raw_pos.truck_dev
    ADD COLUMN truck_age NUMBER(4);


-- Sessão 6: Passo 2 - Popular Coluna
UPDATE latam_frostbyte_tasty_bytes.raw_pos.truck_dev t
    SET truck_age = (YEAR(CURRENT_DATE()) / t.year);


-- Sessão 6: Passo 3 - Consultar conteúdo da nova coluna
SELECT
    t.truck_id,
    t.year,
    t.truck_age
FROM latam_frostbyte_tasty_bytes.raw_pos.truck_dev t;


/*----------------------------------------------------------------------------------
Sessão de início rápido 7: usando "time travel" para recuperação de desastres

   Embora tenhamos cometido um erro, Snowflake tem muitos recursos que podem nos ajudar
   de problemas aqui. O processo que usaremos aproveitará o histórico de consultas, variáveis ​​SQL
   e Time Travel para reverter nossa tabela `truck_dev` para o que parecia antes
   para essa declaração de atualização ruim.
-------------------------------------------------- ---------------------------------*/

-- Sessão 7: Passo 1: usar histórico de consultas
SELECT 
    query_id,
    query_text,
    user_name,
    query_type,
    start_time
FROM TABLE(latam_frostbyte_tasty_bytes.information_schema.query_history())
WHERE 1=1
    AND query_type = 'UPDATE'
    AND query_text LIKE '%latam_frostbyte_tasty_bytes.raw_pos.truck_dev%'
ORDER BY start_time DESC;


-- Sessão 7: Passo 2 - armazenar ID do ultimo Update
SET query_id = 
(
    SELECT TOP 1 query_id
    FROM TABLE(latam_frostbyte_tasty_bytes.information_schema.query_history())
    WHERE 1=1
        AND query_type = 'UPDATE'
        AND query_text LIKE '%SET truck_age = (YEAR(CURRENT_DATE()) / t.year);'
    ORDER BY start_time DESC
);

-- validar Query ID
select $query_id;



-- Sessão 7: Passo 3 - Recriar tabela com uso do time travel
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.truck_dev
    AS 
SELECT * FROM latam_frostbyte_tasty_bytes.raw_pos.truck_dev
BEFORE(STATEMENT => $query_id); 


--tabela restauranda para o momento imediatamente antes do update ser executado.
SELECT * FROM latam_frostbyte_tasty_bytes.raw_pos.truck_dev t
ORDER BY t.truck_id;


/*----------------------------------------------------------------------------------
Sessão de início rápido 8: Uso de viagem no tempo para recuperação de dados em caso de desastres

   Embora tenhamos cometido um erro, Snowflake tem muitos recursos que podem nos ajudar
   de problemas aqui. O processo que usaremos aproveitará o histórico de consultas, variáveis ​​SQL
   e Time Travel para reverter nossa tabela `truck_dev` para o que parecia antes
   para essa declaração de atualização ruim.
-------------------------------------------------- ---------------------------------*/


-- Sessão 8: Passo 1 - Adición de valores calculados correctamente a nuestra columna
UPDATE latam_frostbyte_tasty_bytes.raw_pos.truck_dev t
SET truck_age = (YEAR(CURRENT_DATE()) - t.year);


-- Sessão 8: Passo 2 - Swap entre a tabela dev e prod
USE ROLE sysadmin;

ALTER TABLE latam_frostbyte_tasty_bytes.raw_pos.truck_dev 
    SWAP WITH latam_frostbyte_tasty_bytes.raw_pos.truck;


-- Sessão 8: Passo 3 - Validar produção
SELECT
    t.truck_id,
    t.year,
    t.truck_age
FROM latam_frostbyte_tasty_bytes.raw_pos.truck t
WHERE t.make = 'Ford';


/*----------------------------------------------------------------------------------
Sessão de início rápido 9: Deletar tabelas

   Podemos dizer oficialmente que nosso desenvolvedor concluiu com sucesso a tarefa atribuída.
   Com a coluna truck_age instalada e calculada corretamente, nosso administrador de sistema pode
   limpe as mesas que sobraram para terminar as coisass.
-------------------------------------------------- ---------------------------------*/

-- Section 9: Step 1 - Eliminar tabela
DROP TABLE latam_frostbyte_tasty_bytes.raw_pos.truck;

-- consulta-la
select * from latam_frostbyte_tasty_bytes.raw_pos.truck;


-- Section 9: Step 2 - Restaurar tabela
UNDROP TABLE latam_frostbyte_tasty_bytes.raw_pos.truck;

-- Consultar TB de novo
select * from latam_frostbyte_tasty_bytes.raw_pos.truck;

-- Section 9: Step 3 - Eliminar 
DROP TABLE latam_frostbyte_tasty_bytes.raw_pos.truck_dev;




/**************************************************************************/
/*------               Quickstart Reset Scripts                     ------*/
/**************************************************** *********************/
USE ROLE accountadmin;
UPDATE latam_frostbyte_tasty_bytes.raw_pos.truck SET make = 'Ford_' WHERE make = 'Ford';
ALTER TABLE latam_frostbyte_tasty_bytes.raw_pos.truck DROP COLUMN truck_age;
UNSET query_id;
 