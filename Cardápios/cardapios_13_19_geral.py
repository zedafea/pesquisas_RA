# -*- coding: utf-8 -*-
"""
Created on Thu May 20 15:07:23 2021

@author: guilh
"""

import pandas as pd
from unidecode import unidecode

card_15 = pd.read_csv(r'C:\Users\guilh\Documents\Pesquisa\CARDAPIOS_15_19\Tabulação Cardápios - 2015.csv')
df_15 = pd.read_excel(r'C:\Users\guilh\Documents\Pesquisa\tabelas_censo_escolar.xlsx',sheet_name='Tabela_2015',
                   engine='openpyxl')

df_card = pd.read_csv(r'C:\Users\guilh\Documents\Pesquisa\Tabulação Cardápios - 2015_2019 dir_ensino.csv')
df_13_19_1 = pd.read_csv(r'C:\Users\guilh\Documents\Pesquisa\2013-19 - dados_tratado.csv')
df_refeicao = pd.read_csv(r'C:\Users\guilh\Documents\Pesquisa\alimentos - alimentos.csv')
df_ali_class = pd.read_csv(r'C:\Users\guilh\Documents\Pesquisa\banco_cardapios_2007_2013_2019_072920 - lista alimentos.csv')

#to_do = retirar espaços,acentos etc para facilitar o merge

#Merge dos dados dos cardápios com os alimentos dos cardápios
dfs = []
for i in df_card.columns[2:]:
    if i[0] == 'c':
        ano = i[5:]
        dfs.append(pd.merge(df_card,df_13_19_1[df_13_19_1['ano'] == int(ano)],how='left',left_on=[f'card_{ano}',f'agrup_{ano}'],
         right_on=['tipo_cardapio','agrup']))
main_v1 = pd.concat(dfs)

#Merge com tipo de refeição (lanche/almoço)
df_refeicao = df_refeicao[['index','refeição']]
main_v1 = pd.merge(main_v1,df_refeicao,how='left',left_on=['alimento_original'],right_on=['index']) 

#Merge com as colunas de classificação dos alimentos (ultraprocessado,protetor etc)
df_ali_class.rename(columns = {'classif_NOVA_2':'ultraprocessado'},inplace = True)
df_ali_class['ultraprocessado'] = 0
df_ali_class.loc[(df_ali_class['classif_NOVA'] == 4),'ultraprocessado'] = 1
df_ali_class['alimento'] = df_ali_class['alimento'].str.lower()
df_ali_class['alimento'] = df_ali_class['alimento'].apply(lambda s:unidecode(s) if type(s) == str else s)
df_ali_class = df_ali_class.iloc[:,1:5]
df_ali_class.sort_values(by=['alimento'],inplace=True)
df_ali_class = df_ali_class.reset_index(drop=True)
df_ali_class = df_ali_class.drop(df_ali_class.index[[39,317,544,545,547,548,1289,1292,1295,1299]])
df_ali_class = df_ali_class.dropna()
df_ali_class = df_ali_class.drop_duplicates(subset=['alimento'])
main_v1['alimento_original'] = main_v1['alimento_original'].str.lower()
main_v1['alimento_original'] = main_v1['alimento_original'].apply(lambda s:unidecode(s) if type(s) == str else s)
main_v2 = pd.merge(main_v1,df_ali_class,how='left',left_on=['alimento_original'],
                 right_on = ['alimento'])

main_v2.dropna(subset=['alimento_original'],inplace=True)
main_v2 = main_v2[['index_x','ano','data','alimento','refeição','classif_NOVA','ultraprocessado','alim_protetores']]

#Testes 21/05
d_15 = df_13_19_1[df_13_19_1['ano'] == 2015]
a = d_15['tipo_cardapio'].value_counts().reset_index()

