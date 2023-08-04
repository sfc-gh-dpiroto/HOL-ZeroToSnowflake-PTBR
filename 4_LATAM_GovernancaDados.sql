/***************************************************************************************************
       

  _        _  _____  _    __  __   ____        _            
 | |      / \|_   _|/ \  |  \/  | | __ ) _   _| |_ ___  ___ 
 | |     / _ \ | | / _ \ | |\/| | |  _ \| | | | __/ _ \/ __|
 | |___ / ___ \| |/ ___ \| |  | | | |_) | |_| | ||  __/\__ \
 |_____/_/   \_\_/_/   \_\_|  |_| |____/ \__, |\__\___||___/
                                         |___/              
     
                         
Quickstart:   Tasty Bytes - Latam Zero to Snowflake - Gobierno de Datos
Version:      v1
Script:       VOHL_LATAM.sql         
Create Date:  2023-05-16
Author:       Carlos Suarez
Copyright(c): 2023 Snowflake Inc. All rights reserved.
*****************************************************************************************************/

-- Sessão  3:  1 - Contexto
USE ROLE accountadmin;

USE WAREHOUSE latam_build_wh;

alter warehouse latam_build_wh set warehouse_size = 'xsmall' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;


-- Sessão  3:  2 - Visualizar Roles da conta
SHOW ROLES;


-- Sessão  3:  3 - Roles de sistema
SELECT 
    "name",
    "comment"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" IN ('ORGADMIN','ACCOUNTADMIN','SYSADMIN','USERADMIN','SECURITYADMIN','PUBLIC');


/*----------------------------------------------------------------------------------
Início rápido - Sessão 3: 
3: Usando a análise de resultados para filtrar nossa saída Sessão 
4: Criando uma função e concedendo privilégios

    Agora que entendemos essas funções definidas pelo sistema, vamos começar a aproveitá-las para
    crie uma função de teste e conceder a eles acesso aos dados de fidelidade do cliente que implementaremos   
    nossos recursos iniciais de governança de dados e nossa loja taste_dev_wh
-------------------------------------------------- ---------------------------------*/

-- Sessão  4:  1: Criar nova Role de testes
USE ROLE useradmin;

CREATE OR REPLACE ROLE latam_tasty_test_role
    COMMENT = 'test role for tasty bytes';

-- Sessão  4:  2: conceder permissões de uso do Warehouse para nova role
USE ROLE securityadmin;
GRANT OPERATE, USAGE ON WAREHOUSE latam_build_wh TO ROLE latam_tasty_test_role;


-- Sessão  4:  3: conceder privilégios de DB e Schema a nova role.
GRANT USAGE ON DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_test_role;
GRANT USAGE ON ALL SCHEMAS IN DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_test_role;


-- Sessão  4:  4: conceder privilégios de acesso a objetos do DB
GRANT SELECT ON ALL TABLES IN SCHEMA latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_test_role;
GRANT SELECT ON ALL TABLES IN SCHEMA latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_test_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_test_role;


-- Sessão  4:  5 - conceder acesso a nova role para meu usuário atual
SET my_user_var  = CURRENT_USER();
GRANT ROLE latam_tasty_test_role TO USER identifier($my_user_var);


/*----------------------------------------------------------------------------------
Sessão de início rápido 4 - Criar e anexar Tags às nossas colunas PII

   O primeiro conjunto de recursos de governança de dados que queremos implementar e testar será o :
   - Mascaramento de dados dinâmicos com base em rótulos. Esse recurso nos permitirá mascarar dados PII         em colunas no tempo de execução da consulta de nossa função de teste, mas deixe-as expostas para         roles privilegiadas.

   Antes de podermos começar a mascarar dados, vamos primeiro explorar quais PII existem em nosso
   Dados de fidelização de clientes.
-------------------------------------------------- ---------------------------------*/

-- Sessão  4:  1 - Encontrar dados PII (Personal Identifiable Information) - Dado Sensível
USE ROLE latam_tasty_test_role;
USE WAREHOUSE latam_build_wh;

SELECT 
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.city,
    cl.country
FROM latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty cl;


-- Sessão  4:  2 - Criação de Tags
USE ROLE accountadmin;

CREATE OR REPLACE TAG latam_frostbyte_tasty_bytes.raw_customer.pii_name_tag
    COMMENT = 'PII Tag for Name Columns';
    
CREATE OR REPLACE TAG latam_frostbyte_tasty_bytes.raw_customer.pii_phone_number_tag
    COMMENT = 'PII Tag for Phone Number Columns';
    
CREATE OR REPLACE TAG latam_frostbyte_tasty_bytes.raw_customer.pii_email_tag
    COMMENT = 'PII Tag for E-mail Columns';


-- Sessão  4 -  3 - Aplicação de Tags
ALTER TABLE latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty 
    MODIFY COLUMN first_name 
        SET TAG latam_frostbyte_tasty_bytes.raw_customer.pii_name_tag = 'First Name';

ALTER TABLE latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty 
    MODIFY COLUMN last_name 
        SET TAG latam_frostbyte_tasty_bytes.raw_customer.pii_name_tag = 'Last Name';

ALTER TABLE latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty 
    MODIFY COLUMN phone_number 
        SET TAG latam_frostbyte_tasty_bytes.raw_customer.pii_phone_number_tag = 'Phone Number';

ALTER TABLE latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty 
    MODIFY COLUMN e_mail
        SET TAG latam_frostbyte_tasty_bytes.raw_customer.pii_email_tag = 'E-mail Address';


-- Sessão  4:  4 - Exploração de Tags
SELECT 
    tag_database,
    tag_schema,
    tag_name,
    column_name,
    tag_value 
FROM TABLE(latam_frostbyte_tasty_bytes.information_schema.tag_references_all_columns
    ('latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty','table'));


/*----------------------------------------------------------------------------------
Sessão de início rápido 5: Criação e aplicação de políticas de mascaramento baseadas em tags

   Com nossas tags base instaladas, agora podemos começar a desenvolver o Dynamic Masking.
   Políticas para oferecer suporte a diferentes requisitos de mascaramento para nosso nome, número de telefone  e colunas de e-mail.
-------------------------------------------------- ---------------------------------*/

-- Sessão  5:  1 - Criação de políticas de Mascaramento
USE ROLE sysadmin;

CREATE OR REPLACE MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.name_mask AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN val
    ELSE '**~MASKED~**'
END;

CREATE OR REPLACE MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.phone_mask AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN val
    ELSE CONCAT(LEFT(val,3), '-***-****')
END;

CREATE OR REPLACE MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.email_mask AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN val
    ELSE CONCAT('**~MASKED~**','@', SPLIT_PART(val, '@', -1))
END;
            

-- Sessão  5:  2 - Aplicação de tags para políticas de mascaramento
USE ROLE accountadmin;

ALTER TAG latam_frostbyte_tasty_bytes.raw_customer.pii_name_tag 
    SET MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.name_mask;
    
ALTER TAG latam_frostbyte_tasty_bytes.raw_customer.pii_phone_number_tag
    SET MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.phone_mask;
    
ALTER TAG latam_frostbyte_tasty_bytes.raw_customer.pii_email_tag
    SET MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.email_mask;


/*----------------------------------------------------------------------------------
Sessão de início rápido 6: Testando nossas políticas de mascaramento baseadas em tags

   Com a implementação de nossas Políticas de Mascaramento Baseadas em tags, vamos validar o que
   realizamos até agora para confirmar que conseguimos nos encontrar com o cliente da Tasty Bytes
   Requisitos de mascaramento de dados PII da Fidelity.
-------------------------------------------------- ---------------------------------*/

-- Sessão  6:  1: testar políticas de mascaramento com um usuário com restrições de acesso
USE ROLE latam_tasty_test_role;
USE WAREHOUSE latam_build_wh;

SELECT 
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    cl.city,
    cl.country
FROM latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty cl
WHERE cl.country IN ('United States','Canada','Brazil');


-- Sessão  6:  2: Testar políticas de mascaramento
SELECT TOP 10
    clm.customer_id,
    clm.first_name,
    clm.last_name,
    clm.phone_number,
    clm.e_mail,
    SUM(clm.total_sales) AS lifetime_sales_usd
FROM latam_frostbyte_tasty_bytes.analytics.customer_loyalty_metrics_v clm
WHERE clm.city = 'San Mateo'
GROUP BY clm.customer_id, clm.first_name, clm.last_name, clm.phone_number, clm.e_mail
ORDER BY lifetime_sales_usd DESC;


-- Sessão  6:  3: testar políticas de mascaramento com um usuário SEM restrições de acesso
USE ROLE accountadmin;

SELECT TOP 10
    clm.customer_id,
    clm.first_name,
    clm.last_name,
    clm.phone_number,
    clm.e_mail,
    SUM(clm.total_sales) AS lifetime_sales_usd
FROM latam_frostbyte_tasty_bytes.analytics.customer_loyalty_metrics_v clm
WHERE 1=1
    AND clm.city = 'San Mateo'
GROUP BY clm.customer_id, clm.first_name, clm.last_name, clm.phone_number, clm.e_mail
ORDER BY lifetime_sales_usd DESC;


/*----------------------------------------------------------------------------------
Sessão de início rápido 7 - Implementação e políticas de segurança em nível de linha

Satisfeito com nosso mascaramento dinâmico baseado em tags que controla o mascaramento no nível da coluna,agora veremos como restringir o acesso no nível da linha para nossa função de teste.

Dentro de nossa tabela de fidelidade do cliente, nossa função deve ver apenas os clientes que são
baseado em **Tókio**. Felizmente, o Snowflake tem outra poderosa governança de dados nativa
recurso que pode lidar com isso em escala chamado Row Access Policies.

Para nosso caso de uso, aproveitaremos a abordagem da tabela de mapeamento.
-------------------------------------------------- ---------------------------------*/

-- Sessão  7:  1 - Tabela de Mapeamento
USE ROLE sysadmin;

CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.public.row_policy_map
    (role STRING, city_permissions STRING);

    
-- Sessão  7:  2 - definir regra de acesso role + valor de linha
INSERT INTO latam_frostbyte_tasty_bytes.public.row_policy_map
    VALUES ('LATAM_TASTY_TEST_ROLE','Tokyo');


select * from latam_frostbyte_tasty_bytes.public.row_policy_map;    

-- Sessão  7:  3: Criação de política de acesso a linhas
CREATE OR REPLACE ROW ACCESS POLICY latam_frostbyte_tasty_bytes.public.customer_city_row_policy
       AS (city STRING) RETURNS BOOLEAN ->
       CURRENT_ROLE() IN -- lista de roles que no estarán sujetos a la política  
           (
            'ACCOUNTADMIN','SYSADMIN'
           )
        OR EXISTS -- esta cláusula faz referência à nossa tabela de mapeamento acima para lidar com a filtragem em nível de linha
            (
            SELECT rp.role 
                FROM latam_frostbyte_tasty_bytes.public.row_policy_map rp
            WHERE 1=1
                AND rp.role = CURRENT_ROLE()
                AND rp.city_permissions = city
            );

            
-- Sessão  7:  4: Politicas de acesso a linhas de uma tabela
ALTER TABLE latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty
    ADD ROW ACCESS POLICY latam_frostbyte_tasty_bytes.public.customer_city_row_policy ON (city);

    
-- Sessão  7:  5: validar políticas de acesso em uma role com acessos restritos
USE ROLE latam_tasty_test_role;

SELECT 
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.city,
    cl.marital_status,
    DATEDIFF(year, cl.birthday_date, CURRENT_DATE()) AS age
FROM latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty cl
GROUP BY cl.customer_id, cl.first_name, cl.last_name, cl.city, cl.marital_status, age;


-- Sessão  7:  6: validar politica de acesso de linha
SELECT 
    clm.city,
    SUM(clm.total_sales) AS total_sales_usd
FROM latam_frostbyte_tasty_bytes.analytics.customer_loyalty_metrics_v clm
GROUP BY clm.city;


-- Sessão  7:  7: validar políticas de acesso com uma role com super poderes
USE ROLE sysadmin;

SELECT 
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.city,
    cl.marital_status,
    DATEDIFF(year, cl.birthday_date, CURRENT_DATE()) AS age
FROM latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty cl
GROUP BY cl.customer_id, cl.first_name, cl.last_name, cl.city, cl.marital_status, age;







/**************************************************************************/
/*------               Quickstart Reset Scripts                     ------*/
/**************************************************** *********************/

USE ROLE accountadmin;

DROP ROLE IF EXISTS latam_tasty_test_role;

ALTER TAG latam_frostbyte_tasty_bytes.raw_customer.pii_name_tag UNSET MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.name_mask;
ALTER TAG latam_frostbyte_tasty_bytes.raw_customer.pii_phone_number_tag UNSET MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.phone_mask;
ALTER TAG latam_frostbyte_tasty_bytes.raw_customer.pii_email_tag UNSET MASKING POLICY latam_frostbyte_tasty_bytes.raw_customer.email_mask;

DROP TAG IF EXISTS latam_frostbyte_tasty_bytes.raw_customer.pii_name_tag;
DROP TAG IF EXISTS latam_frostbyte_tasty_bytes.raw_customer.pii_phone_number_tag;
DROP TAG IF EXISTS latam_frostbyte_tasty_bytes.raw_customer.pii_email_tag;

ALTER TABLE latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty
DROP ROW ACCESS POLICY latam_frostbyte_tasty_bytes.public.customer_city_row_policy;

DROP TABLE IF EXISTS latam_frostbyte_tasty_bytes.public.row_policy_map;