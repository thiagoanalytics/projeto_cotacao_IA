import yfinance as yf
from datetime import datetime
import pandas as pd


commodities = ['GC=F', 'SI=F', 'CL=F', 'NG=F', 'HG=F', 'ZC=F', 'ZW=F', 'ZS=F', 'KC=F', 'CT=F', 'SB=F'] 
# Ouro, Prata, Petróleo, Gás Natural, Cobre, Milho, Trigo, Soja, Café, Algodão, Açúcar

def get_commodities_prices():
    # Criar um dataframe vazio para armazenar os dados
    df = pd.DataFrame()

    for i in commodities:
        ultimo_df = yf.Ticker(i).history(period='1d', interval='1m')[['Close']].tail(1)
        ultimo_df = ultimo_df.rename(columns={'Close': 'preco'})
        ultimo_df['ativo'] = i
        ultimo_df['moeda'] = 'USD'
        ultimo_df['horario_de_coleta'] = datetime.now()
        ultimo_df = ultimo_df[['preco', 'ativo', 'moeda', 'horario_de_coleta']]

        df = pd.concat([df, ultimo_df], ignore_index=True)


    return df



