# Importanto as bibliotecas necessárias
import requests
import certifi
import pandas as pd

import extrator
from io import BytesIO
import sys
import ast

df = pd.read_csv('candidatos.csv', dtype=object)

df['texto'] = ""
df['tamanho'] = ""
df['excec'] = ""
df['erro'] = ""

df2 = df
df = df.head(10)

with requests.Session() as s:
    for i in range(len(df)):
        df.at[i,'tamanho'] = []
        df.at[i,'excec'] = []
        url_arqs = ast.literal_eval(df.loc[i, 'url_proposta'])
        texto_final = ""

        for j in url_arqs:
            r = s.get(j, verify=certifi.where(), timeout=500)
            t = r.content

            if sys.getsizeof(t) < 8000000:

                extracao = BytesIO(t)
                texto_ext = extrator.Extrator(extracao)
                texto_ext = " ".join(texto_ext.split())
                texto_final = f"{texto_final} {texto_ext}"

            elif sys.getsizeof(t) > 8000000:
                df.at[i, 'tamanho'].append(j)

            else:
                df.at[i, 'excec'].append(j)

        df.at[i,'texto'] = texto_final

df.to_csv('teste.csv')