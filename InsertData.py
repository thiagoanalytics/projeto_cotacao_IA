from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
import os

def insert_data_to_db(df):
    # Carregar variáveis de ambiente do arquivo .env
    load_dotenv()

    # Configurar a conexão com o banco de dados
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')  
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')

    #criar conexão com o sqlalchemy
    database_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(database_url)

    # Renomear para o nome correto no banco
    df_envio = df.rename(columns={
        "horario_de_coleta": "horario_coleta"
    })[["ativo", "preco", "moeda", "horario_coleta"]]

    try:
        df_envio.to_sql("stg_cotacoes", engine, schema="stage", if_exists="append", index=False)
        print("Dados inseridos com sucesso!")
    except Exception as e:
        print(f"Erro ao inserir dados: {e}")
