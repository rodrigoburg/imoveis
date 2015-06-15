from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
from pymongo import MongoClient
import pymongo
from os import listdir
from os.path import isfile, join
import csv
from pygeocoder import Geocoder
import json
import time
from pandas import DataFrame, read_csv
import numpy as np
import collections

def le_arquivos(mypath):
    return [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]

def conecta(db):
    client = MongoClient()
    my_db = client["imoveis"]
    my_collection = my_db[db]
    return my_collection

def scraper_lista(cidade, uf,pag,conexao,antigos):
    url = 'http://www.zapimoveis.com.br/venda/imoveis/sp+sao-paulo/#{"precomaximo":"2147483647","parametrosautosuggest":[{"Bairro":"","Zona":"","Cidade":"'+cidade+'","Agrupamento":"","Estado":"'+uf+'"}],"pagina":"'+pag+'","paginaOrigem":"ResultadoBusca","formato":"Lista"}'
    print(url)
    request = Request(url)
    request.add_header('User-agent', 'Mozilla/5.0 (Linux i686)')

    #lê página
    page = BeautifulSoup(urlopen(request).read())
    items = page.findAll("div",{"class":"list-cell"})

    adicionados = []
    for item in items:
        classe = item.find("span",{"class":"price"}).string
        if ("consulta" not in classe):
            link = item.find("a")["href"]
            adicionados = adiciona_link(link,conexao,antigos,adicionados)

    return adicionados

def adiciona_link(link,conexao,antigos,adicionados):
    imovel = {}
    imovel["link"] = link
    imovel["_id"] = int(link.split("ID-")[1].split("/")[0])

    if imovel["_id"] not in antigos:
        try:
            conexao.insert(imovel)
            adicionados.append(imovel["_id"])
            print(imovel)
        except pymongo.errors.ConnectionFailure:
            conexao = conecta("imoveis")
            conexao.insert(imovel)
            adicionados.append(imovel["_id"])
            print(imovel)
        except pymongo.errors.DuplicateKeyError:
            print("imóvel já existe mas o if nao pegou")
            print(imovel)

    return adicionados

def scraper_pagina(link,conexao):
    imovel = {}
    request = Request(link)
    request.add_header('User-agent', 'Mozilla/5.0 (Linux i686)')
    page = BeautifulSoup(urlopen(request).read())

    menu = page.find("div",{"class":"side-left"})
    subtitulo = menu.find("h1")

    imovel["endereco"] = str(subtitulo).split("/span>")[1].split("<br/>")[0].strip()
    imovel["bairro"] = subtitulo.find("span",{"class":"logradouro"}).string.split(",")[0].strip()

    preco = menu.find("span",{"class":"value-ficha"})
    imovel["preco"] = str(preco).split("R$")[1].split("</span")[0].strip().replace(".","")

    infos = page.find("div",{"class":"informacoes-imovel"})
    infos = infos.findAll("li")
    try:
        for info in infos:
            if "Valor do m" in info.find("span"):
                imovel["valor_m2"] = str(info).split("<span")[0].split("li>")[1].strip()
            elif info.find("span").string == "quartos":
                imovel["quartos"] = str(info).split("<span")[0].split("li>")[1].strip()
            elif "Área" in info.find("span").string:
                imovel["area"] = str(info).split("<span")[0].split("li>")[1].strip()
    except TypeError:
        return

    try:
        coordenadas = page.find("div",{"id":"imgMapaGoogleEstatico"})["onclick"].split("(")[1].split(", onclick")[0].split(",")
        coordenadas = [float(c) for c in coordenadas]
        imovel["coordenadas"] = coordenadas
    except TypeError:
        pass

    imovel["link"] = link
    imovel["_id"] = int(link.split("ID-")[1].split("/")[0])

    try:
        conexao.insert(imovel)
        print(imovel)
    except pymongo.errors.ConnectionFailure:
        conexao = conecta("imoveis")
        conexao.insert(imovel)
        print(imovel)
    except pymongo.errors.DuplicateKeyError:
        print("imóvel já existe")
        print(imovel)
        return

def main_scraper():
    conexao = conecta("imoveis")
    conexao.remove()
    antigos = []
    cidades = {"SAO PAULO":{"uf":"SP","num_pags":8401,"ja_feitas":0}}
    for cidade in cidades:
        for pag in range(cidades[cidade]["ja_feitas"],cidades[cidade]["num_pags"]):
            pag = str(pag+1)
            print("***********")
            print("FAZENDO PAGINA "+pag)
            print("***********")
            antigos += scraper_lista(cidade,cidades[cidade]["uf"],pag,conexao,antigos)

def acha_id(imovel):
    id = imovel["link"]
    return id


def faz_consulta():
    conexao = conecta("imoveis")
    resultado = conexao.find()
    for r in resultado:
        print(r)

def junta_csvs():
    path = "dados_censo/universo/"
    pastas = listdir(path)
    #abre um arquivo vazio onde vamos juntar as infos
    with open("setores_universo.csv","w") as outfile:
        escritor = csv.writer(outfile, delimiter=',')

        for p in pastas:
            if p not in ["docs",".DS_Store","TO"]:
                print("Fazendo os arquivos do:"+p)
                novo_path = path+p+"/"
                novo_path = listdir(novo_path)[0]
                novo_path = path+p+"/"+novo_path+"/CSV/"

                if p == "SP Capital":
                    p = "SP1"


                #agora abrimos os arquivos de onde vamos tirar as infos
                with open(novo_path+"Basico_"+p+".csv","r", encoding="iso-8859-1") as basico, \
                    open(novo_path+"Domicilio01_"+p+".csv","r", encoding="iso-8859-1") as domicilio, \
                    open(novo_path+"DomicilioRenda_"+p+".csv","r", encoding="iso-8859-1") as renda, \
                    open(novo_path+"Entorno01_"+p+".csv","r", encoding="iso-8859-1") as entorno:
                    leitores = {
                        "basico":csv.reader(basico, delimiter=';'),
                        "domicilio":csv.reader(domicilio, delimiter=';'),
                        "renda":csv.reader(renda, delimiter=';'),
                        "entorno":csv.reader(entorno, delimiter=';')
                    }
                    headers = acha_headers(leitores)
                    indices = acha_indices(headers)

                    novo_header = list(indices["basico"].keys()) + list(indices["domicilio"].keys()) \
                                  + list(indices["renda"].keys()) + list(indices["entorno"].keys())

                    escritor.writerow(novo_header)

                    #vamos iterar por todos os arquivos
                    while True:
                        linha = {}
                        linha_a_escrever = []
                        row = {}
                        try:
                            row["basico"] = next(leitores["basico"])
                            row["domicilio"] = next(leitores["domicilio"])
                            row["renda"] = next(leitores["renda"])
                            row["entorno"] = next(leitores["entorno"])

                            for arquivo in indices:
                                for variavel in indices[arquivo]:
                                    linha[variavel] = row[arquivo][indices[arquivo][variavel]]

                            for variavel in novo_header:
                                linha_a_escrever.append(linha[variavel])

                            escritor.writerow(linha_a_escrever)
                        except StopIteration:
                            break


def acha_headers(leitores):
    headers = {}
    for arquivo in leitores:
        headers[arquivo] = next(leitores[arquivo])
    return headers

def acha_indices(headers):
    saida = {"renda":{},"domicilio":{},"entorno":{},"basico":{}}
    for arquivo in headers:
        header = headers[arquivo]
        if arquivo == "basico":
            saida[arquivo]["cod_setor"] = header.index("Cod_setor")
            saida[arquivo]["uf"] = header.index("Nome_da_UF ")
            saida[arquivo]["mun"] = header.index("Nome_do_municipio")
            saida[arquivo]["situacao"] = header.index("Situacao_setor")
            saida[arquivo]["num_domicilios"] = header.index("V001")
        if arquivo == "domicilio":
            saida[arquivo]["abastecimento_agua"] = header.index("V012")
            saida[arquivo]["coleta_lixo"] = header.index("V035")
        if arquivo == "renda":
            saida[arquivo]["renda_total"] = header.index("V002")
        if arquivo == "entorno":
            saida[arquivo]["identificacao_rua"] = header.index("V002")
            saida[arquivo]["iluminacao_publica"] = header.index("V008")
            saida[arquivo]["pavimentacao"] = header.index("V014")
            saida[arquivo]["calcada"] = header.index("V020")
            saida[arquivo]["meio_fio"] = header.index("V026")
            saida[arquivo]["bueiro"] = header.index("V032")
            saida[arquivo]["acessibilidade"] = header.index("V038")
            saida[arquivo]["arborizacao"] = header.index("V044")
            saida[arquivo]["esgoto_ceu_aberto"] = header.index("V050")
            saida[arquivo]["lixo_acumulado"] = header.index("V050")
    return saida

def acha_coordenadas_bairros():
    #with open("dados_imoveis/dados_filtrados.json","r") as jsonfile:
    with open("dados_imoveis/dados_bairro_com_geo.json","r") as jsonfile:

        dados = json.load(jsonfile)

    Capital = {}
    Capital["acre"] = "rio branco"
    Capital["alagoas"] = "maceio"
    Capital["amapa"] = "macapa"
    Capital["amazonas"] = "manaus"
    Capital["bahia"] = "salvador"
    Capital["ceara"] = "fortaleza"
    Capital["distrito federal"] = "brasilia"
    Capital["espirito santo"] = "vitoria"
    Capital["goias"] = "goiania"
    Capital["internacional"] = "miami"
    Capital["maranhao"] = "sao luis"
    Capital["mato grosso"] = "cuiaba"
    Capital["mato grosso do sul"] = "campo grande"
    Capital["minas gerais"] = "belo horizonte"
    Capital["para"] = "belem"
    Capital["paraiba"] = "joao pessoa"
    Capital["parana"] = "curitiba"
    Capital["pernambuco"] = "recife"
    Capital["piaui"] = "teresina"
    Capital["rio de janeiro"] = "rio de janeiro"
    Capital["rio grande do norte"] = "natal"
    Capital["rio grande do sul"] = "porto alegre"
    Capital["rondonia"] = "porto velho"
    Capital["roraima"] = "boa vista"
    Capital["santa catarina"] = "florianopolis"
    Capital["sao paulo"] = "sao paulo"
    Capital["sergipe"] = "aracaju"
    Capital["tocantins"] = "palmas"

    estados = dict((v,k) for k,v in Capital.items())

    for capital in dados:
        print("Estamos em: "+capital)
        uf = estados[capital]

        for bairro in dados[capital]:
            if bairro.lower() not in ["indeterminado","indiferente"] and "coordenadas" not in dados[capital][bairro]:
                while True:
                    time.sleep(1)
                    try:
                        results = Geocoder.geocode(bairro+", "+capital+", "+uf)
                        print("Bairro com sucesso: "+bairro)
                        break
                    except:
                    #except (ConnectionResetError, TimeoutError, OSError):
                        print("Ops! Erro tentando pegar geolocalização. Tentando de novo...")
                        pass

                coordenadas = results[0].coordinates
                dados[capital][bairro]["coordenadas"] = coordenadas

        with open("dados_imoveis/dados_bairro_com_geo.json","w") as outfile:
            json.dump(dados, outfile,ensure_ascii=False)


def filtra_bairros():
    with open("dados_imoveis/dados_bairro.json","r") as jsonfile:
        dados = json.load(jsonfile)

    saida = {}
    anos = [2010,2011,2012,2013,2014]
    for uf in dados:
        for capital in dados[uf]:
            saida[capital] = {}
            for i in range(len(dados[uf][capital]["bairro"])):

                bairro = dados[uf][capital]["bairro"][i]
                data = dados[uf][capital]["dados"][i]
                novos_dados = {}
                if data:
                    for ano in anos:
                        novos_dados[ano] = []
                    for d in data:
                        if d["Ano"] in anos:
                            novos_dados[d["Ano"]].append(d)

                    for ano in novos_dados:
                        valor = acha_media_lista_dict(novos_dados[ano],"Valor")
                        amostra = acha_media_lista_dict(novos_dados[ano],"Amostra")
                        if amostra > 0:
                            if bairro["Texto"] not in saida[capital]:
                                saida[capital][bairro["Texto"]] = {}
                            saida[capital][bairro["Texto"]][ano] = {"valor":valor,"amostra":amostra}
    for cidade in saida:
        print("*******")
        print("Para "+cidade+" temos os seguintes bairros e anos:")
        for bairro in saida[cidade]:
            for ano in saida[cidade][bairro]:
                print("Bairro "+bairro+ " - "+str(ano))

    with open("dados_imoveis/dados_filtrados.json","w") as outfile:
        json.dump(saida, outfile,ensure_ascii=False)


def acha_media_lista_dict(lista,key):
    saida = 0
    tamanho = 0
    for item in lista:
        if item["Amostra"] > 0:
            saida += item[key]
            tamanho +=1

    return saida / tamanho if tamanho > 0 else 0

def exporta_bairros_csvs():
    with open("dados_imoveis/dados_bairro_com_geo.json","r") as jsonfile:
        dados = json.load(jsonfile)

    saida = {"cidade":[],"bairro":[],"ano":[],"valor":[],"lat":[],"long":[]}
    for cidade in dados:
        for bairro in dados[cidade]:
            if "2010" in dados[cidade][bairro]:
                if "coordenadas" not in dados[cidade][bairro]:
                    print("Bairro sem coordenadas: "+bairro+" - "+cidade)
                    continue

                saida["cidade"].append(cidade)
                saida["bairro"].append(bairro)
                saida["ano"].append(2010)
                saida["valor"].append(dados[cidade][bairro]["2010"]["valor"])
                saida["lat"].append(dados[cidade][bairro]["coordenadas"][0])
                saida["long"].append(dados[cidade][bairro]["coordenadas"][1])

    df = DataFrame(saida)

    df.to_csv("dados_imoveis/dados_total_2010.csv",index=False)
    #df = df[df.cidade == "sao paulo"]
    #df.to_csv("dados_imoveis/dados_total_2010_sp.csv",index=False)

def filtra_dados_censo():
    dados = read_csv("dados_censo/setores_universo.csv")
    cidades = ["SÃO PAULO","BRASÍLIA","RIO DE JANEIRO","BELO HORIZONTE","PORTO ALEGRE","CURITIBA","SALVADOR","RECIFE","FORTALEZA","BELÉM","MANAUS"]
    zona_urbana = ["1","2","3"]
    dados = dados[dados.mun.isin(cidades)]
    dados = dados[dados.situacao.isin(zona_urbana)]
    dados.to_csv("dados_censo/setores_universo.csv",index=False)

def acha_indice(setor):
    setor = dict(setor)
    variaveis_fora = ["cod_setor","mun","uf","situacao","num_domicilios","renda_total"]
    saida = []
    for variavel in setor:
        if variavel not in variaveis_fora:
            try:
                saida.append(int(setor[variavel])/int(setor["num_domicilios"]))
            except ValueError: #se houver algum erro no valor, como X ou NaN
                return "incalculavel"
    return int(np.average(saida)*100)

def calcula_indice_setores():
    dados = read_csv("dados_censo/setores_universo.csv")
    dados["indice_urbanismo"] = dados.apply(acha_indice,axis=1)
    dados = dados[dados.indice_urbanismo != "incalculavel"]
    dados.to_csv("dados_censo/indice_urbanismo.csv",index=False)

def junta_indice_bairros():
    dados_indice = read_csv("dados_censo/indice_urbanismo.csv")
    dados_bairros = read_csv("dados_merged.csv")
    colunas = ["cod_setor","valor"]
    for coluna in dados_bairros.columns:
        if coluna not in colunas:
            del dados_bairros[coluna]

    dados_bairros.index = dados_bairros["cod_setor"]
    del dados_bairros["cod_setor"]
    dados_bairros = dados_bairros.to_dict()
    dados_bairros = dados_bairros["valor"]

    dados_indice["valor"] = dados_indice["cod_setor"].apply(lambda t:int(dados_bairros[t]) if t in dados_bairros else "erro")
    dados_indice.to_csv("dados_final_capitais_principais.csv",index=False)

def calcula_desigualdade():
    setores = read_csv("dados_censo/setores_universo_sp_urbanismo.csv")
    lista = list(setores["indice_urbanismo"])
    calculos = collections.OrderedDict()
    calculos["media_1_superior"] = np.average(np.percentile(lista,99))
    calculos["media_5_superior"] = np.average(np.percentile(lista,95))
    calculos["media_10_superior"] = np.average(np.percentile(lista,90))
    calculos["media_40_inferior"]= np.average(np.percentile(lista,40))
    calculos["media_10_inferior"] = np.average(np.percentile(lista,10))
    calculos["gini"] = calcula_gini(lista)
    calculos["palma"] = calculos["media_10_superior"]/calculos["media_40_inferior"]

    print("INDICE DE URBANIZACAO")
    for variavel in calculos:
        print(variavel +": "+str(calculos[variavel]))
    print()


def calcula_gini(list_of_values):
  sorted_list = sorted(list_of_values)
  height, area = 0, 0
  for value in sorted_list:
    height += value
    area += height - value / 2.
  fair_area = height * len(list_of_values) / 2
  return (fair_area - area) / fair_area

#main_scraper()
#faz_consulta()
#filtra_bairros()
#acha_coordenadas_bairros()
#exporta_bairros_csvs()
#filtra_dados_censo()
#calcula_indice_setores()
junta_indice_bairros()
#"CD_GEOCODM" LIKE '%4314902%' or "CD_GEOCODM" LIKE '%4106902%' or "CD_GEOCODM" LIKE '%3550308%' or "CD_GEOCODM" LIKE '%3304557%' or "CD_GEOCODM" LIKE '%2611606%' or "CD_GEOCODM" LIKE '%2927408%' or "CD_GEOCODM" LIKE '%5300108%' or "CD_GEOCODM" LIKE '%3106200%' or "CD_GEOCODM" LIKE '%1302603%' or "CD_GEOCODM" LIKE '%1501402%' or "CD_GEOCODM" LIKE '%2304400%'
#calcula_desigualdade()
