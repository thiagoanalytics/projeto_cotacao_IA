import pandas as pd
import time
from GetCommodities import get_commodities_prices
from GetPriceBitcoin import get_price_bitcoin
from InsertData import insert_data_to_db

sleep_seconds = 600  # 10 minutes

def main():
    # Obter preços das commodities
    commodities_df = get_commodities_prices()
    
    # Obter preço do Bitcoin
    bitcoin_df = get_price_bitcoin()
    
    # Concatenar os dataframes
    combined_df = pd.concat([commodities_df, bitcoin_df], ignore_index=True)
    
    # Exportar o dataframe combinado para um arquivo CSV
    # combined_df.to_csv('prices.csv', index=False, encoding='utf-8', header=True)

    #Enviar os dados para o banco de dados
    insert_data_to_db(combined_df)

if __name__ == "__main__":

    #main()
    while True:
        main()
        time.sleep(sleep_seconds)   