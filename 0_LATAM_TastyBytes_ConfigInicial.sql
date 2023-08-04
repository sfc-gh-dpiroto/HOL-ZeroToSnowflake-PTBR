/***************************************************************************************************
       

  _        _  _____  _    __  __   ____        _            
 | |      / \|_   _|/ \  |  \/  | | __ ) _   _| |_ ___  ___ 
 | |     / _ \ | | / _ \ | |\/| | |  _ \| | | | __/ _ \/ __|
 | |___ / ___ \| |/ ___ \| |  | | | |_) | |_| | ||  __/\__ \
 |_____/_/   \_\_/_/   \_\_|  |_| |____/ \__, |\__\___||___/
                                         |___/              
     
                         
Quickstart:   Tasty Bytes - Latam Zero to Snowflake - Instalación
Version:      v1
Script:       TastyBytes_LATAM_Instalacion.sql         
Create Date:  2023-05-16
Author:       Carlos Suarez
Copyright(c): 2023 Snowflake Inc. All rights reserved.
*****************************************************************************************************/

USE ROLE sysadmin;

/*--
 Criação das bases de Databases e Scheamas
--*/

-- DB latam_frostbyte_tasty_bytes 
CREATE OR REPLACE DATABASE latam_frostbyte_tasty_bytes;

-- Criação schema  raw_pos 
CREATE OR REPLACE SCHEMa latam_frostbyte_tasty_bytes.raw_pos;
-- Criação schema  raw_customer 
CREATE OR REPLACE SCHEMa latam_frostbyte_tasty_bytes.raw_customer;
-- Criação schema  harmonized 
CREATE OR REPLACE SCHEMa latam_frostbyte_tasty_bytes.harmonized;
-- Criação schema  analytics 
CREATE OR REPLACE SCHEMa latam_frostbyte_tasty_bytes.analytics;




-- Criação de wharehouse 
CREATE OR REPLACE WAREHOUSE latam_build_wh
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'warehouse demo parparatam testybyte';


alter warehouse latam_build_wh set warehouse_size = 'xxxlarge' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;


-- Criação de roles
USE ROLE securityadmin;

-- Criação de  roles funcionais
CREATE ROLE IF NOT EXISTS latam_tasty_admin
    COMMENT = 'admin parparatam tasty bytes';
    
CREATE ROLE IF NOT EXISTS latam_tasty_data_engineer
    COMMENT = 'data engineer parparatam tasty bytes';
      
CREATE ROLE IF NOT EXISTS latam_tasty_data_scientist
    COMMENT = 'data scientist parparatam tasty bytes';
    
CREATE ROLE IF NOT EXISTS latam_tasty_bi
    COMMENT = 'business intelligence parparatam tasty bytes';
    
CREATE ROLE IF NOT EXISTS latam_tasty_data_app
    COMMENT = 'data application developer parparatam tasty bytes';
    
CREATE ROLE IF NOT EXISTS latam_tasty_dev
    COMMENT = 'developer parparatam tasty bytes';
    
-- Hierarquia de Roles
GRANT ROLE latam_tasty_admin TO ROLE sysadmin;
GRANT ROLE latam_tasty_data_engineer TO ROLE latam_tasty_admin;
GRANT ROLE latam_tasty_data_scientist TO ROLE latam_tasty_admin;
GRANT ROLE latam_tasty_bi TO ROLE latam_tasty_admin;
GRANT ROLE latam_tasty_data_app TO ROLE latam_tasty_admin;
GRANT ROLE latam_tasty_dev TO ROLE latam_tasty_data_engineer;



-- Permissões Adicionais 
USE ROLE accountadmin;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE latam_tasty_data_engineer;

GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE latam_tasty_admin;


-- Permissões a Objetos
USE ROLE securityadmin;

GRANT USAGE ON DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_admin;
GRANT USAGE ON DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_data_engineer;
GRANT USAGE ON DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_data_scientist;
GRANT USAGE ON DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_bi;
GRANT USAGE ON DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_data_app;
GRANT USAGE ON DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_dev;

GRANT USAGE ON ALL SCHEMAS IN DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_admin;
GRANT USAGE ON ALL SCHEMAS IN DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_data_engineer;
GRANT USAGE ON ALL SCHEMAS IN DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_data_scientist;
GRANT USAGE ON ALL SCHEMAS IN DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_bi;
GRANT USAGE ON ALL SCHEMAS IN DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_data_app;
GRANT USAGE ON ALL SCHEMAS IN DATABASE latam_frostbyte_tasty_bytes TO ROLE latam_tasty_dev;

GRANT ALL ON schema latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_admin;
GRANT ALL ON schema latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_data_engineer;
GRANT ALL ON schema latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_data_scientist;
GRANT ALL ON schema latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_bi;
GRANT ALL ON schema latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_data_app;
GRANT ALL ON schema latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_dev;

GRANT ALL ON schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_admin;
GRANT ALL ON schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_engineer;
GRANT ALL ON schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_scientist;
GRANT ALL ON schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_bi;
GRANT ALL ON schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_app;
GRANT ALL ON schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_dev;

GRANT ALL ON schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_admin;
GRANT ALL ON schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_engineer;
GRANT ALL ON schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_scientist;
GRANT ALL ON schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_bi;
GRANT ALL ON schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_app;
GRANT ALL ON schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_dev;



-- Permissões a warehouse
GRANT ALL ON WAREHOUSE latam_build_wh TO ROLE sysadmin;
GRANT OWNERSHIP ON WAREHOUSE latam_build_wh TO ROLE latam_tasty_admin REVOKE CURRENT GRANTS;

GRANT ALL ON WAREHOUSE latam_build_wh TO ROLE latam_tasty_admin;
GRANT ALL ON WAREHOUSE latam_build_wh TO ROLE latam_tasty_data_engineer;
GRANT ALL ON WAREHOUSE latam_build_wh TO ROLE latam_tasty_bi;
GRANT ALL ON WAREHOUSE latam_build_wh TO ROLE latam_tasty_data_scientist;
GRANT ALL ON WAREHOUSE latam_build_wh TO ROLE latam_tasty_data_app;
GRANT ALL ON WAREHOUSE latam_build_wh TO ROLE latam_tasty_dev;





-- Privilegios futuros  
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_admin;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_data_engineer;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_data_scientist;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_bi;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_data_app;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.raw_pos TO ROLE latam_tasty_dev;

GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_admin;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_data_engineer;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_data_scientist;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_bi;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_data_app;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_dev;

GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_admin;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_engineer;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_scientist;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_bi;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_app;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_dev;

GRANT ALL ON FUTURE VIEWS IN schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_admin;
GRANT ALL ON FUTURE VIEWS IN schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_engineer;
GRANT ALL ON FUTURE VIEWS IN schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_scientist;
GRANT ALL ON FUTURE VIEWS IN schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_bi;
GRANT ALL ON FUTURE VIEWS IN schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_data_app;
GRANT ALL ON FUTURE VIEWS IN schema latam_frostbyte_tasty_bytes.harmonized TO ROLE latam_tasty_dev;

GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_admin;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_engineer;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_scientist;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_bi;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_app;
GRANT ALL ON FUTURE TABLES IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_dev;

GRANT ALL ON FUTURE VIEWS IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_admin;
GRANT ALL ON FUTURE VIEWS IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_engineer;
GRANT ALL ON FUTURE VIEWS IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_scientist;
GRANT ALL ON FUTURE VIEWS IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_bi;
GRANT ALL ON FUTURE VIEWS IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_app;
GRANT ALL ON FUTURE VIEWS IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_dev;

GRANT ALL ON FUTURE FUNCTIONS IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_scientist;

GRANT USAGE ON FUTURE PROCEDURES IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_admin;
GRANT USAGE ON FUTURE PROCEDURES IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_engineer;
GRANT USAGE ON FUTURE PROCEDURES IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_scientist;
GRANT USAGE ON FUTURE PROCEDURES IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_bi;
GRANT USAGE ON FUTURE PROCEDURES IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_data_app;
GRANT USAGE ON FUTURE PROCEDURES IN schema latam_frostbyte_tasty_bytes.analytics TO ROLE latam_tasty_dev;




/********
Inicio de processo de proteção de dados 
********/


-- Aplicar Tag para classificação de dados 
GRANT CREATE TAG ON schema latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_admin;
GRANT CREATE TAG ON schema latam_frostbyte_tasty_bytes.raw_customer TO ROLE latam_tasty_data_engineer;


--Link de tag a futuras políticas de mascaramento 
USE ROLE accountadmin;
GRANT APPLY TAG ON ACCOUNT TO ROLE latam_tasty_admin;
GRANT APPLY TAG ON ACCOUNT TO ROLE latam_tasty_data_engineer;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE latam_tasty_admin;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE latam_tasty_data_engineer;




/******
Inicio de conexão a fonte de dados externas (AWS S3) que contém arquivos no formato CSV
******/



  
-- Inserção de dados no schema raw_pos 
USE ROLE sysadmin;
USE WAREHOUSE latam_build_wh;


--Criação de tipo de formato de arquivo (file format)  CSV
CREATE OR REPLACE FILE FORMAT latam_frostbyte_tasty_bytes.public.csv_ff 
type = 'csv';



--Criação de stage (AWS S3) que contem os dados de Origem
CREATE OR REPLACE STAGE latam_frostbyte_tasty_bytes.public.s3load
COMMENT = 'Quickstarts S3 Stage Connection'
url = 's3://sfquickstarts/frostbyte_tastybytes/'
file_format = latam_frostbyte_tasty_bytes.public.csv_ff;


list @latam_frostbyte_tasty_bytes.public.s3load;

-- Criação de modelo  de dados (tabela) para dados crus (raw zone)


-- Criação tabela pais (country) 
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.country
(
    country_id NUMBER(18,0),
    country VARCHAR(16777216),
    iso_currency VARCHAR(3),
    iso_country VARCHAR(2),
    city_id NUMBER(19,0),
    city VARCHAR(16777216),
    city_population VARCHAR(16777216)
);

-- Criação tabela franquias
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.franchise 
(
    franchise_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216) 
);

-- Criação tabela localizações (location)
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.location
(
    location_id NUMBER(19,0),
    placekey VARCHAR(16777216),
    location VARCHAR(16777216),
    city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    country VARCHAR(16777216)
);

-- Criação table menú (productos)
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);

-- Criação tabela ponto de venda (truck)   
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.truck
(
    truck_id NUMBER(38,0),
    menu_type_id NUMBER(38,0),
    primary_city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_region VARCHAR(16777216),
    country VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    franchise_flag NUMBER(38,0),
    year NUMBER(38,0),
    make VARCHAR(16777216),
    model VARCHAR(16777216),
    ev_flag NUMBER(38,0),
    franchise_id NUMBER(38,0),
    truck_opening_date DATE
);

-- Criação tabela de pedidios (order_header)
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.order_header
(
    order_id NUMBER(38,0),
    truck_id NUMBER(38,0),
    location_id FLOAT,
    customer_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    shift_id NUMBER(38,0),
    shift_start_time TIME(9),
    shift_end_time TIME(9),
    order_channel VARCHAR(16777216),
    order_ts TIMESTAMP_NTZ(9),
    served_ts VARCHAR(16777216),
    order_currency VARCHAR(3),
    order_amount NUMBER(38,4),
    order_tax_amount VARCHAR(16777216),
    order_discount_amount VARCHAR(16777216),
    order_total NUMBER(38,4)
);


-- Criação table de detalhes de pedidios (order_detail)  
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_pos.order_detail 
(
    order_detail_id NUMBER(38,0),
    order_id NUMBER(38,0),
    menu_item_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    line_number NUMBER(38,0),
    quantity NUMBER(5,0),
    unit_price NUMBER(38,4),
    price NUMBER(38,4),
    order_item_discount_amount VARCHAR(16777216)
);

-- Criação table de fidelicdade de cliente (customer loyalty)  
CREATE OR REPLACE TABLE latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty
(
    customer_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    postal_code VARCHAR(16777216),
    preferred_language VARCHAR(16777216),
    gender VARCHAR(16777216),
    favourite_brand VARCHAR(16777216),
    marital_status VARCHAR(16777216),
    children_count VARCHAR(16777216),
    sign_up_date DATE,
    birthday_date DATE,
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216)
);





/*
Criação de Views da camada Harmonizada (harmonized view)
*/



-- Criação de views: Pedidos (orders_v) 
CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.harmonized.orders_v
    AS
SELECT 
    oh.order_id,
    oh.truck_id,
    oh.order_ts,
    od.order_detail_id,
    od.line_number,
    m.truck_brand_name,
    m.menu_type,
    t.primary_city,
    t.region,
    t.country,
    t.franchise_flag,
    t.franchise_id,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name,
    l.location_id,
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.children_count,
    cl.gender,
    cl.marital_status,
    od.menu_item_id,
    m.menu_item_name,
    od.quantity,
    od.unit_price,
    od.price,
    oh.order_amount,
    oh.order_tax_amount,
    oh.order_discount_amount,
    oh.order_total
FROM latam_frostbyte_tasty_bytes.raw_pos.order_detail od
JOIN latam_frostbyte_tasty_bytes.raw_pos.order_header oh
    ON od.order_id = oh.order_id
JOIN latam_frostbyte_tasty_bytes.raw_pos.truck t
    ON oh.truck_id = t.truck_id
JOIN latam_frostbyte_tasty_bytes.raw_pos.menu m
    ON od.menu_item_id = m.menu_item_id
JOIN latam_frostbyte_tasty_bytes.raw_pos.franchise f
    ON t.franchise_id = f.franchise_id
JOIN latam_frostbyte_tasty_bytes.raw_pos.location l
    ON oh.location_id = l.location_id
LEFT JOIN latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty cl
    ON oh.customer_id = cl.customer_id;




    
-- Criação de view: métricas de Fidelidade (loyalty_metrics_v) 
CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.harmonized.customer_loyalty_metrics_v
    AS
SELECT 
    cl.customer_id,
    cl.city,
    cl.country,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    SUM(oh.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT oh.location_id) AS visited_location_ids_array
FROM latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty cl
JOIN latam_frostbyte_tasty_bytes.raw_pos.order_header oh
ON cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
cl.last_name, cl.phone_number, cl.e_mail;






/*
 Criação de views do schema Analitico  
*/

-- Criação de view: Pedidios (orders_v) para schema de Analítica (analytics)
CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.analytics.orders_v
COMMENT = 'Tasty Bytes Order Detail View'
    AS
SELECT DATE(o.order_ts) AS date, * FROM latam_frostbyte_tasty_bytes.harmonized.orders_v o;


-- Criação de view: metricas de fidelidade cliente (customer_loyalty_metrics_v)  para schema analítico (analytics) 
CREATE OR REPLACE VIEW latam_frostbyte_tasty_bytes.analytics.customer_loyalty_metrics_v
COMMENT = 'Tasty Bytes Customer Loyalty Member Metrics View'
    AS
SELECT * FROM latam_frostbyte_tasty_bytes.harmonized.customer_loyalty_metrics_v;






/*****
 Carga de dados da stage externan(AWS S3) para Snowflake  
******/


alter warehouse latam_build_wh set WAREHOUSE_SIZE = 'LARGE' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;


-- Carga de dados para tabela country 
COPY INTO latam_frostbyte_tasty_bytes.raw_pos.country
FROM @latam_frostbyte_tasty_bytes.public.s3load/raw_pos/country/;

-- Carga de dados para tabela franquicias (franchise) 
COPY INTO latam_frostbyte_tasty_bytes.raw_pos.franchise
FROM @latam_frostbyte_tasty_bytes.public.s3load/raw_pos/franchise/;

-- location table load
COPY INTO latam_frostbyte_tasty_bytes.raw_pos.location
FROM @latam_frostbyte_tasty_bytes.public.s3load/raw_pos/location/;

-- Carga de dados para tabela (menu) 
COPY INTO latam_frostbyte_tasty_bytes.raw_pos.menu
FROM @latam_frostbyte_tasty_bytes.public.s3load/raw_pos/menu/;

-- Carga de dados para tabela puntos de venta (truck) 
COPY INTO latam_frostbyte_tasty_bytes.raw_pos.truck
FROM @latam_frostbyte_tasty_bytes.public.s3load/raw_pos/truck/;

-- Carga de dados para tabela lealtad de clientes (customer_loyalty) 
COPY INTO latam_frostbyte_tasty_bytes.raw_customer.customer_loyalty
FROM @latam_frostbyte_tasty_bytes.public.s3load/raw_customer/customer_loyalty/;

-- Carga de dados para tabela pedidos (order_header) 
COPY INTO latam_frostbyte_tasty_bytes.raw_pos.order_header
FROM @latam_frostbyte_tasty_bytes.public.s3load/raw_pos/order_header/;

-- Carga de dados para tabela detalle de pedidos (order_detail) 
COPY INTO latam_frostbyte_tasty_bytes.raw_pos.order_detail
FROM @latam_frostbyte_tasty_bytes.public.s3load/raw_pos/order_detail/;


-- redimencionar WH latam_build_wh
alter warehouse latam_build_wh set WAREHOUSE_SIZE = 'xsmall' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;


-- setup completado 
SELECT 'Instalação Completa' AS note;