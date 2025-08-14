import pandas as pd
import json
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

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

# Carregar configurações
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

for base in config["bases"]:
    csv_path = base["csv_path"]
    schema = base["schema"]
    table = base["table"]
    column_mapping = base["column_mapping"]

    print(f"📂 Processando: {csv_path} → {schema}.{table}")

    # Ler CSV
    df = pd.read_csv(csv_path)

    # Renomear colunas
    df_mapeado = df.rename(columns=column_mapping)

    # Selecionar apenas colunas do destino
    colunas_destino = list(column_mapping.values())
    df_mapeado = df_mapeado[colunas_destino]

    # Enviar para o banco
    df_mapeado.to_sql(table, engine, schema=schema, if_exists="append", index=False)

    print(f"✅ {len(df_mapeado)} registros enviados para {schema}.{table}\n")

print("🏁 Processo concluído!")

