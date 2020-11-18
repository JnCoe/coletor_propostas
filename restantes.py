# Importanto as bibliotecas necessárias
import concurrent.futures
import requests
import certifi
import pandas as pd
import numpy as np
import time

import extrator
from io import BytesIO
import sys
import ast


df4 = pd.read_csv('resultado_final2.csv', dtype=object)

def extraido(numero):
    url_arqs = ast.literal_eval(df4.loc[numero, 'url_proposta'])
    texto_final = ""

    for j in url_arqs:
        r = s.get(j, verify=certifi.where(), timeout=500)
        t = r.content

        if sys.getsizeof(t) < 8000000:

            extracao = BytesIO(t)

            try:
                texto_ext = extrator.convert_pdf_to_txt(extracao)
                texto_ext = " ".join(texto_ext.split())
                texto_final = f"{texto_final} {texto_ext}"

            except:
                return "#ERRO"

        elif sys.getsizeof(t) > 8000000:
            return "#TAMANHO"

        else:
            return "#EXCECAO"

    print(f"#{numero} {df4.at[numero, 'nm_candidato']} concluído")
    
    dici_final = [numero,texto_final]

    return dici_final


start = time.perf_counter()

with requests.Session() as s:
    with concurrent.futures.ProcessPoolExecutor() as executor:
        lista = []
        
        for linha in range(len(df4)):
            if type(df4.loc[linha, 'texto']) == float and df4.loc[linha, 'url_proposta'] != '[]':
                lista.append(linha)
                print(f"\nVazio: #{linha} {df4.loc[linha, 'nm_candidato']}")
        
        print(f"\n\nTOTAL VAZIOS: {len(lista)}")             
                
        texto_cache = [executor.submit(extraido, i) for i in lista]
        
        for f in texto_cache:
            f = f.result()
            df4.at[f[0],'texto'] = f[1]
        


finish = time.perf_counter()

print(f"tempo: {round(finish-start, 2)}")

df4.to_csv('final_final2.csv')