import pandas as pd
import numpy as np
import urllib
import requests
import janitor
import json
import certifi

candidatos = pd.read_csv("Dados/consulta_cand_2020_BRASIL.csv", sep=";", encoding='latin-1', dtype=object)
candidatos = janitor.clean_names(candidatos)

candidatos = candidatos[candidatos.ds_cargo == "PREFEITO"].reset_index()
candidatos['url_proposta'] = ""
candidatos['id_divulga'] = ""

link_1 = "https://divulgacandcontas.tse.jus.br/divulga/rest/v1/candidatura/buscar/2020/"
link_2 = "/2030402020/candidato/"

link_busca_1 = "https://divulgacandcontas.tse.jus.br/divulga/rest/v1/candidatura/listar/2020/"
link_busca_2 = "/2030402020/11/candidatos"

with requests.Session() as s:
    for i in range(len(candidatos)):

        print(i)

        candidatos.at[i, 'url_proposta'] = []

        id_ue = candidatos.loc[i, "sg_ue"]
        nr_cand = int(candidatos.loc[i, "nr_candidato"])
        id_cand = int(candidatos.loc[i, "sq_candidato"])
        url = f"{link_1}{id_ue}{link_2}{id_cand}"

        r = s.get(url, verify=certifi.where(), timeout=20)

        try:
            dados_cand = json.loads(r.content)

        except:
            url_busca = f"{link_busca_1}{id_ue}{link_busca_2}"
            r_busca = s.get(url_busca, verify=certifi.where(), timeout=20)
            dados_busca = json.loads(r_busca.content)

            for k in dados_busca['candidatos']:
                if k['numero'] == nr_cand:
                    id_cand = k['id']

            url = f"{link_1}{id_ue}{link_2}{id_cand}"

            r = s.get(url, verify=certifi.where(), timeout=20)
            dados_cand = json.loads(r.content)

        for j in dados_cand['arquivos']:
            if j['codTipo'] == '5' and j['tipo'] == 'pdf':
                nome_arq = urllib.parse.quote(j['nome'])
                end_arq = j['url']

                url_final = f"https://divulgacandcontas.tse.jus.br/{end_arq}{nome_arq}"

                print(dados_cand['nomeCompleto'])

                candidatos.at[i, 'id_divulga'] = id_cand
                candidatos.at[i, 'url_proposta'].append(url_final)
                print("break")

candidatos.to_csv('candidatos_linkados.csv')
