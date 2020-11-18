"""Esse script irá tomar como argumento um arquivo contendo uma lista de palavras e irá realizar uma busca no google
por cada uma delas, retornando os resultados da primeira página (link, título e descrição). Há a possibilidade de
especificar uma expressão constante como por exemplo buscar dentro de uma URL específica. O resultado será salvo no
arquivo "Output.csv" em csv encapsulado por aspas e separado por ";"."""

from colorama import Fore, Back, Style

# Importanto as bibliotecas necessárias
import requests
from bs4 import BeautifulSoup

import pandas as pd

import numpy as np
from janitor.functions import clean_names
import pickle

import extrator
from io import BytesIO
import sys


class Pdfer():
    def __init__(self, arquivo):
        df = pd.read_csv(arquivo)

        temp_texto = pd.read_csv("compilado2.csv")

        temp_texto = temp_texto[['4','texto']]

        print("DFs criado!")

        df = result = pd.merge(df, temp_texto, how='left', on='4')

        headers = {"user-agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:65.0) Gecko/20100101 Firefox/65.0"}
        model_file = open("Modelo_01_10", 'rb')
        text_clf = pickle.load(model_file)

        with requests.Session() as s:

            for index, row in df.iterrows():
                url = row['4']
                texto = row['texto']
                desc = str(df['5'][index])
                df['5'][index] = " ".join(desc.split())

                if "#Nao_e_HTML" in row['texto']:
                    print(f"{index} é PDF")
                    try:
                        r = s.get(url, verify=False, timeout=500)
                        content_type = r.headers.get('content-type')
                        j = r.content

                        if 'application/pdf' in content_type and sys.getsizeof(j) < 8000000:

                            i = BytesIO(j)
                            x = extrator.Extrator(i)
                            x = " ".join(x.split())
                            x = [x]
                            previsao = text_clf.predict_proba(x)[:,1][0]

                            df['previsao'][index] = f"{previsao}_TESTE"

                        elif 'application/pdf' in content_type and sys.getsizeof(j) > 8000000:
                            df['previsao'][index] = f"#Excedeu_tamanho"

                    
                    except:
                        df['previsao'][index] = f"#ERRO"


                else:
                    print(f"{index} não é PDF")

        df = df.drop('texto', axis=1)
        df.to_csv("teste.csv")


    def iterador(self, linha):

        url = linha['4']
        texto = linha['texto']

        if "#Nao_e_HTML" in linha['texto']:

            print(f"{index} é PDF")

            r = s.get(url, verify=False, timeout=500)
            content_type = r.headers.get('content-type')

            if 'application/pdf' in content_type:

                i = BytesIO(r.content)
                x = extrator.Extrator(i)
                x = " ".join(x.split())
                x = [x]
                previsao = text_clf.predict_proba(x)[:,1][0]

                df['previsao'][index] = f"{previsao} _TESTE"


        else:
            print("teste?")


# Uso de exemplo
#Scraper('termos.csv', 'site:*.leg.br').buscador()

#busca_api().google_search('algoritmo site:*.gov.br')

#Busca_api('Dados/termos1e2.csv', 'site:*.mp.br', paginas=5, pag_inicial=6).buscador()

#Scraper2()

Scraper2()
