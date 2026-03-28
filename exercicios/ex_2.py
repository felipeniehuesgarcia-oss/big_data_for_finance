import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os

load_dotenv()

def create_db_engine():
    user     = quote_plus(os.getenv('DB_USER', 'postgres'))
    password = quote_plus(os.getenv('DB_PASSWORD', 'password'))
    host     = os.getenv('DB_HOST', 'localhost')
    port     = os.getenv('DB_PORT', '5432')
    dbname   = os.getenv('DB_NAME', 'data_lake')
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(conn_str)

engine = create_db_engine()
print("✓ Engine criada com sucesso.")

tabelas = ['dfp_cia_aberta_bpa_con', 'dfp_cia_aberta_bpa_ind', 'dfp_cia_aberta_bpp_con', 
'dfp_cia_aberta_bpp_ind', 'dfp_cia_aberta_composicao_capital', 'dfp_cia_aberta_dfc_md_con', 
'dfp_cia_aberta_dfc_md_ind', 'dfp_cia_aberta_dfc_mi_con', 'dfp_cia_aberta_dfc_mi_ind', 
'dfp_cia_aberta_dmpl_con', 'dfp_cia_aberta_dmpl_ind', 'dfp_cia_aberta_dra_con', 
'dfp_cia_aberta_dra_ind', 'dfp_cia_aberta_dre_con', 'dfp_cia_aberta_dre_ind', 
'dfp_cia_aberta_dva_con', 'dfp_cia_aberta_dva_ind']

def create_query(table):
    return f"""
with cte as (
select	
	"CNPJ_CIA",
	"DT_REFER",
	MAX("VERSAO"::INTEGER) as versao_max
from 
	layer_01_bronze.{table}
group by
	"CNPJ_CIA",
	"DT_REFER"
)
select 
	"CNPJ_CIA",
	SUM(case when "versao_max" > 1 then 1 else 0 END) as periodos_c_reapresentacao,
	AVG(versao_max) as media_versoes,
	MAX(versao_max) as max_versao
from
	cte
group by "CNPJ_CIA"
order by 2 desc
"""

def executar_para_todas_as_tabelas(array_tabelas):
    engine = create_db_engine()
    for table in tabelas:
        query = create_query(table)
        with engine.connect() as conn:
            df = pd.read_sql(
                text(query),
                con=conn
            )
            print(df.head(10))

executar_para_todas_as_tabelas(tabelas)
        
