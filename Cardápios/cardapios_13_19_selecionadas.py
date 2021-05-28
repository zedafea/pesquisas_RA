# -*- coding: utf-8 -*-
"""
Created on Tue Apr 27 14:48:44 2021

@author: guilh
"""
#considerar só estaduais
#to_do: comentar código, verificar escolas que fecharam (deixar como NA),arrumar flocos de milhos hidratados

#Importação das bibliotecas 
import pandas as pd
from unidecode import unidecode

#Importação dos dados
df_molde = pd.read_stata(r'C:\Users\guilh\Documents\Pesquisa\cardapios_silvia_tratado.dta')
df_alimentos = pd.read_stata(r'C:\Users\guilh\Documents\Pesquisa\classificacao_1319.dta')
df_13_19_2 = pd.read_csv(r'C:\Users\guilh\Documents\Pesquisa\2013-19 - lista alimentos.csv')

df_ali_class = pd.read_csv(r'C:\Users\guilh\Documents\Pesquisa\banco_cardapios_2007_2013_2019_072920 - lista alimentos.csv')
df_refeicao = pd.read_csv(r'C:\Users\guilh\Documents\Pesquisa\alimentos - alimentos.csv')
df_novo = pd.read_csv(r'C:\Users\guilh\Documents\Pesquisa\escolas localizacao.xlsx - escolas - Silvia.csv') 
df_13_19_1 = pd.read_csv(r'C:\Users\guilh\Documents\Pesquisa\2013-19 - dados_tratado.csv')
df_censo = pd.read_stata(r'C:\Users\guilh\Documents\Pesquisa\escolas_censo_2007-2019.dta')
df_compara = pd.read_stata(r'C:\Users\guilh\Documents\Pesquisa\cardapio_0719.dta')

#Manipulação básica
df_molde['cod_escola'] = df_molde['cod_escola'].astype('int')
df_novo = df_novo.iloc[0:43,0:25]
df_novo['cod_escola'] = df_novo['cod_escola'].astype('int')
df_novo['agrup_2015'] = df_novo['agrup_2015'].astype('float64')
df_novo = df_novo.drop(df_novo.columns[6:10],1)
df_novo = df_novo.dropna()
df_13_19_1 = df_13_19_1[df_13_19_1['ano'] >= 2015]

#Juntando os textos das colunas do df_novo que servirão para darmos match depois
for j in range(6,21):
    if df_novo.columns[j].startswith('t') or df_novo.columns[j].startswith('c'):
        name = df_novo.columns[j]
        df_novo[name] = df_novo[name].str.lower().str.replace(" ","").str.replace(",","").str.replace(".","").str.replace("-","").str.replace(":","")

#Juntando os textos das colunas do df_13_19_1 que servirão para darmos match depois
df_13_19_1['tipo_cardapio'] = df_13_19_1['tipo_cardapio'].str.lower().str.replace(" ","").str.replace(",","").str.replace(".","").str.replace("-","").str.replace(":","")
df_13_19_1['cidades_agrup'] = df_13_19_1['cidades_agrup'].str.lower().str.replace(" ","").str.replace(",","").str.replace(".","").str.replace("-","").str.replace(":","")

#Lidando com o problema dos tipo_cardapio que não dão matching (explicitado no for da próxima seção)
df_novo.loc[(df_novo['nome_escola'] == 'BERTHA CORREA E CASTRO DA ROCHA' ) |
            (df_novo['nome_escola'] == 'JARDIM SANTA ANGELA' )
            , 'tipo_cardapio_18'] = 'emergencialeteccardápioemergenciallote2merendasecaindividual'
df_novo.loc[(df_novo['nome_escola'] == 'WALTER ROCHA CAMARGO PROFESSOR EMEF'),
            'tipo_cardapio_19'] = 'cardápioregularsemcongeladoregião2'
comp = df_13_19_1.loc[(df_13_19_1['ano'] == 2016) &
               (df_13_19_1['agrup'] == 3) &
               (df_13_19_1['cidades_agrup'].str.contains('andradina') == True),:]
comp['tipo_cardapio'].value_counts()

#Juntando os dfs df_novo e df_13_19_1 por meio das colunas 'tipo_cardapio' e 'agrup'
dfs = []
for i in df_novo.columns[6:]:
    if i[0] == 'c':
        ano = i[14:]
        dfs.append(pd.merge(df_novo,df_13_19_1[df_13_19_1['ano'] == int(ano)],how='left',left_on=[f'tipo_cardapio_{ano[2:]}',f'agrup_{ano}'],
         right_on=['tipo_cardapio','agrup']))
main_v1 = pd.concat(dfs)

#2)Problema do matching de cardápios
val_falt = {}
for c in range(2015,2020):
    aux = main_v1[main_v1['ano'] == c]
    count = aux['nome_escola'].value_counts()
    val_falt[c] = count
val_falt = pd.DataFrame(val_falt)

#Acrescentando a informação do 'horário' da refeição
main_v1['alimento_original'] = main_v1['alimento_original'].str.lower()
main_v1['alimento_original'] = main_v1['alimento_original'].apply(lambda s:unidecode(s) if type(s) == str else s)
main_v1 = pd.merge(main_v1,df_refeicao,how='left',left_on=['alimento_original'],right_on=['index']) 
main_v1 = main_v1.sort_values(by=['nome_escola','ano'])
main_v2 = main_v1[['cod_escola','cidade','nome_cidade','nome_escola',
                     'diretoria_2020','tipo','ano','data','alimento_original_x',
                     'item','considerar','refeição']]
main_v2.rename(columns = {'alimento_original_x':'alimento_original'},inplace=True)
main_v3 = main_v2.dropna(axis=0,subset=['alimento_original'])

#Lidando com o df_ali_class referente a classificação dos alimentos 
#Criando a dummy para ultraprocessados
df_ali_class.rename(columns = {'classif_NOVA_2':'ultraprocessado'},inplace = True)
df_ali_class['ultraprocessado'] = 0
df_ali_class.loc[(df_ali_class['classif_NOVA'] == 4),'ultraprocessado'] = 1
df_ali_class['alimento'] = df_ali_class['alimento'].str.lower()
df_ali_class['alimento'] = df_ali_class['alimento'].apply(lambda s:unidecode(s) if type(s) == str else s)
df_ali_class = df_ali_class.iloc[:,1:5]
    
rep = df_ali_class[df_ali_class.duplicated('alimento')]
#agua, carne moida, flocos de milho, sal
#0,0,4,0
#39,317,544,545,547,548,1292,1295,1299
df_ali_class.sort_values(by=['alimento'],inplace=True)
df_ali_class = df_ali_class.reset_index(drop=True)
df_ali_class = df_ali_class.drop(df_ali_class.index[[39,317,544,545,547,548,1289,1292,1295,1299]])
df_ali_class = df_ali_class.dropna()
#duplicatas na classificação dos alimentos
df_ali_class = df_ali_class.drop_duplicates(subset=['alimento'])

#Junção do main_v4 com a informação de processamento e proteção dos alimentos
main_v4 = pd.merge(main_v3,df_ali_class,how='left',left_on=['alimento_original'],
                 right_on = ['alimento'])

#Junção dos dados de 2007 com os novos para termos informações sobre diretoria_2020,cidade e nome da cidade
df_molde = pd.merge(df_molde,df_novo,how='left',left_on=['cod_escola'],right_on=['cod_escola'])
df_molde = df_molde[['cod_escola','cidade_x','nome_cidade_x','tipo_ref_hora','ano','alimento','classif_NOVA',
       'alim_protetores','nome_escola',
       'diretoria_2020', 'tipo']]
df_molde = df_molde.rename(columns={'cidade_x':'cidade','nome_cidade_x':'nome_cidade'})
df_molde['ultraprocessado'] = 0
df_molde.loc[(df_molde['classif_NOVA'] == 4),'ultraprocessado'] = 1
main_v5 = main_v4.append(df_molde)
main_v5.sort_values(by=['cod_escola','ano'],inplace=True)

#Passando a informação do tipo de refeição dos dados de 2007 para o main
main_v5.loc[(main_v5['tipo_ref_hora'].str.contains('Lanche') == True)|
          (main_v5['tipo_ref_hora'].str.contains('Café') == True)
          ,'refeição'] = 'Desjejum / Lanche'
main_v5.loc[(main_v5['tipo_ref_hora'].str.contains('Almoço') == True),'refeição'] = 'Almoço / Jantar'
main_v5 = main_v5.iloc[:,:16]

#Juntando com os dados do censo
df_censo['nome_escola'] = df_censo['nome_escola'].replace(['ANNA TEIXEIRA PRADO ZACHARIAS PROFA',
                                                           'ALFREDO BURKART PROF',
                                                           'PEDRINA PIRES ZADRA PROFA',
                                                           'JOSE LEVY CEL',
                                                           'NANCI CRISTINA DO ESPIRITO SANTO PROFA',
                                                           'NEWTON PIMENTA NEVES PROF',
                                                           'RIVADAVIA MARQUES JUNIOR PROF',
                                                           'EMEF RIVADAVIA MARQUES JUNIOR PROF',
                                                           'ANTONIO CARLOS DE ANDRADA E SILVA',
                                                           'EMEF ANTONIO CARLOS DE ANDRADA E SILVA',
                                                           'FERNANDO GRACIOSO',
                                                           'EMEF FERNANDO GRACIOSO',
                                                           'ANTONIO SAMPAIO VER',
                                                           'EMEFM ANTONIO SAMPAIO VER',
                                                           'MARIA VICENCOTTI CENTRO EDUCACIONAL',
                                                           'MARIA LUIZA MALZONI ROCHA LEITE DONA EMEIPG',
                                                           'WALTER ROCHA CAMARGO PROF EMEF',
                                                           'SYLVIA ORTHOF EMEIEF',
                                                           'SATURNINO PEREIRA PROF',
                                                           'ANTONIO FRANCISCO PAVANELLO PROF'],
                                                          ['ANNA TEIXEIRA PRADO ZACHARIAS PROFESSORA',
                                                           'ALFREDO BURKART PROFESSOR',
                                                           'PEDRINA PIRES ZADRA PROFESSORA',
                                                           'JOSE LEVY CORONEL',
                                                           'NANCI CRISTINA DO ESPIRITO SANTO PROFESSORA',
                                                           'NEWTON PIMENTA NEVES PROFESSOR',
                                                           'RIVADAVIA MARQUES JR.PROF EMEF',
                                                           'RIVADAVIA MARQUES JR.PROF EMEF',
                                                           'ANTONIO CARLOS DE ANDRADA E SILVA EMEF',
                                                           'ANTONIO CARLOS DE ANDRADA E SILVA EMEF',
                                                           'FERNANDO GRACIOSO EMEF',
                                                           'FERNANDO GRACIOSO EMEF',
                                                           'ANTONIO SAMPAIO VEREADOR EMEFM',
                                                           'ANTONIO SAMPAIO VEREADOR EMEFM',
                                                           'MARIA VICENCOTTI EMEF E EJA',
                                                           'MARIA LUIZA MALZONI ROCHA LEITE DONA EMEIF',
                                                           'WALTER ROCHA CAMARGO PROFESSOR EMEF',
                                                           'SILVIA ORTHOF EMEIEF',
                                                           'SATURNINO PEREIRA PROFESSOR',
                                                           'ANTONIO FRANCISCO PAVANELLO PROFESSOR'])

main_v6 = pd.merge(main_v5,df_censo,how='left',left_on=['cod_escola','ano'],
                 right_on = ['cod_escola','ano'])

main_vf = main_v6[['cod_escola', 'cidade', 'nome_cidade', 'nome_escola_x','cod_orgao_ensino',
                   'diretoria_2020', 'tipo', 'ano', 'data','item',
                   'considerar', 'refeição', 'alimento','classif_NOVA', 'ultraprocessado',
       'alim_protetores','dep_adm', 'localizacao', 'local_func',
       'agua_filtrada', 'agua_publica', 'energia_rede', 'esgoto_rede',
       'coleta_lixo', 'banheiro_pne', 'bilioteca', 'cozinha', 'lab_ciencias',
       'lab_informatica', 'parque_infantil', 'quadra_esportes', 'diretoria',
       'sala_prof', 'sala_especial', 'salas_utilizadas', 'dvd', 'tv',
       'internet', 'pnae', 'funcionarios', 'n_alunos', 'n_docentes']]

main_vf['alimento'] = main_vf['alimento'].str.lower()
main_vf['alimento'] = main_vf['alimento'].apply(lambda s:unidecode(s) if type(s) == str else s)
main_vf.rename(columns={'nome_escola_x':'nome_escola'},inplace=True)

main_vf.to_csv(r'C:\Users\guilh\Documents\Pesquisa\cardapios_2007_2015:2019.csv',index = False)

##Caveats

#2)Com relação ao agrupamento por cardápio:
#ARMEL MIRANDA 2016
#emergencialeteccardápioemergencial (procurado)
#Não encontrei o cardápio emergencial
#agrupamento 3

#BERTHA CORREA E CASTRO DA ROCHA 2018
#emergencialeteccardápioemergencialmerendasecaindividual (procurado)
#emergencialeteccardápioemergenciallote2merendasecaindividual (cardápio mais
#semelhante encontrado)
#agrupamento 2

#JARDIM SANTA ANGELA 2018
#emergencialeteccardápioemergencialmerendasecaindividual (procurado)

#emergencialeteccardápioemergenciallote2merendasecaindividual (cardápio mais 
#semelhante encontrado)
#agrupamento 2

#WALTER ROCHA CAMARGO PROFESSOR EMEF 2019
#regularsemcongeladolote2 (procurado)
#cardápioregularsemcongeladoregião2 (cardápio mais semelhante encontrado)
#agrupamento 2
