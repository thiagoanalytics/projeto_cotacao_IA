from sqlalchemy import create_engine, text
import os
import json
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


for procedure in config["procedures"]:
    procedure_name = procedure["name"]
    print(f"🔄 Executando procedure: {procedure_name}")

    # Executar a procedure
    with engine.connect() as conn:
        conn.execute(text(f"CALL {procedure_name}()"))
        conn.commit()

print("✅ Procedure executada com sucesso!")