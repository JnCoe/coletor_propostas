import pandas as pd
import numpy as np
import urllib
import requests
import janitor
import json
import urllib

candidatos = pd.read_csv("Dados/consulta_cand_2020_BRASIL.csv", sep=";", encoding='latin-1', dtype=object)
candidatos = janitor.clean_names(candidatos)

candidatos = candidatos[candidatos.ds_cargo == "PREFEITO"].reset_index()
candidatos['url_proposta'] = ""

link_1 = "https://divulgacandcontas.tse.jus.br/divulga/rest/v1/candidatura/buscar/2020/"
link_2 = "/2030402020/candidato/"

with requests.Session() as s:
    for i in range(len(candidatos)):
        id_ue = candidatos.loc[i, "sg_ue"]
        id_cand = int(candidatos.loc[i, "sq_candidato"])
        url = f"{link_1}{id_ue}{link_2}{id_cand}"

        r = s.get(url, verify=False, timeout=20)
        dados_cand = json.loads(r.content)

        for i in dados_cand['arquivos']:
            if i['codTipo'] == '5':
                nome_arq = urllib.parse.quote(i['nome'])
                end_arq = i['url']

                url_final = f"https://divulgacandcontas.tse.jus.br/{end_arq}{nome_arq}"

                print(dados_cand['nomeCompleto'])
                print(url_final)

        x = print("break")



print("oi")