# HOL-ZeroToSnowflake-PTBR
Laboratório - Zero to Snowflake

Neste laboratório é possível implementar diferentes casos de uso em uma empresa fictícia chamada TastyBytes usando dados do Snowflake Marketplace (Clima e Geografia) bem como carregar dados de um estágio externo no AWS S3.


## O que é TastyBytes?

Uma rede global de food trucks, com diversas opções de menus regionais, presente em mais de 30 cidades em 15 países.


## Requerimentos
- Conta trial na plataforma Snowflake (https://signup.snowflake.com/) e conhecimentos básicos em SQL.


# Configuração e Carga de Dados 

Passo a passo para configuração inicial da demo (também descrito no vídeo "Configuração e Carga de Dados ") 


## Passo 1: Ativar role ACCOUNTADMIN
![rol accountadmin](imgs/rol.png)

## Passo 2: Carregar o arquivo 0_LATAM_TastyBytes_Configuracion.sql
Opção 1 - Carregar dados na opção "Criar planilha a partir do arquivo SQL", uma janela de seleção do sistema operacional será aberta e escolherá onde o arquivo SQL será armazenado.
![rol accountadmin](imgs/op2.png)
Opção 2 - Carregar dados na opção "Create SQL Worksheet", você pode abrir o arquivo SQL em um navegador, copiar e colar o código na planilha recém-criada.
![rol accountadmin](imgs/op1.png)
## Passo 3: Executar todo código SQL (descrito com mais detalhes em video)
Selecione todo o código SQL e execute tudo de uma única vez.
![rol accountadmin](imgs/op3.png)



# Projeto

Una vez completado el paso 1 y paso 2 de configuración, el proyecto propone 6 etapas, que van desde la creación de aleertas y cuotas de uso de virtual warehouse (cómputo), tranformar datosde origen usando sentencias SQL, implementar de forma ágil el tipo de dato VARIANT de Snowflake para extraer dede JSON en tabla valores específicos, Gobierno de datos para a través de Tags definiendo políticas para enmascarar datos sensibles y privados así como segmentación por renglón, aplicar colaboración de datos para acceder a datos desde Snowflake marketplace para clima y geografía.

- Governança de recursos
<br>1_LATAM_TastyBytes_MonitorRecursos.sql
- Transformação de dados
<br>2_LATAM_TastyBytes_Transformacion.sql
- Dados Semi-Estruturados
<br>3_LATAM_TastyBytes_DatosSemiEstructurados.sql
- Governança de Dados
<br>4_LATAM_GobiernoDatos.sql
- Colaboração
<br>5_LATAM_Colaboracion.sql
- Geoespacial 
<br>6_LATAM_TastyBytes_Geoespacial.sql
