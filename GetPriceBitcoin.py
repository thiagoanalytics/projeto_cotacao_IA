import requests
from datetime import datetime
import pandas as pd

def get_price_bitcoin():
    #url para obter o valor do bitcoin
    url = "https://api.coinbase.com/v2/prices/spot"

    response = requests.get(url)
    data = response.json()

    #extrair os dados do json = preço, ativo, moeda, hora da coleta

    preco = float(data['data']['amount'])
    ativo = data['data']['base']
    moeda = data['data']['currency']
    horario_de_coleta = datetime.now()

    # criar um dataframe com os dados

    df = pd.DataFrame({
        'preco': [preco],
        'ativo': [ativo],
        'moeda': [moeda],
        'horario_de_coleta': [horario_de_coleta]
    })

    # salvar o dataframe em um arquivo csv
    #df.to_csv('cotacao_bitcoin.csv', index=False, encoding='utf-8', header=True)    

    return df