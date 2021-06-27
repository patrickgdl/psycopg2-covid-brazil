# -*- coding: utf-8 -*-
"""
@authors: Patrick Lima e Leonardo Gonçalves
"""

#%% Importar os pacotes de conexão ao BD e Pandas
import psycopg2
import pandas as pd

#%% Credenciais do Banco de Dados e Caminho do arquivo CSV
user = "postgres"
password = "your_password"

csv_path = "C:\\Users\\patri\\casos_brasil.csv"

#%% Conectar no banco de dados
conn = psycopg2.connect(
    user=user,
    password=password,
    dbname='grupo_gamma',
)
cur = conn.cursor()

#%% Função helper de Inserção de Dados no Banco de Dados
def insert_data(sql_query, row_values):
    try:
        cur.execute(sql_query, row_values)
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        print("Query: " + str(cur.query))
        conn.rollback()
        cur.close()
    else:
        conn.commit()

#%% Leitura da base de Casos de COVID e preenchimento de valores faltantes com 0
df_covid = pd.read_csv(csv_path)
df_covid.head()

df_covid = df_covid.fillna(0)

#%% Escolha de 5 cidades e montagem de uma lista de DFs de Cidades
cidades = ['Araucária', 'Quitandinha', 'Uberlândia', 'Canoinhas', 'Gramado']
list_of_cities_dfs = list()
for cidade in cidades:
    list_of_cities_dfs.append(df_covid[df_covid['city'] == cidade])

#%% Concat de toda a lista de 5 Dataframes de Cidades em um único Dataframe 
df_selected_cities = pd.concat(list_of_cities_dfs)

#%% Filtro por Estado, tirando duplicidades para insersão na Tabela de Estado, criação de Id baseado no index resetado (1 a 27)
df_table_states_temp = df_covid.filter(['state'], axis=1).drop_duplicates()

df_table_states = df_table_states_temp.reset_index(drop=True)
df_table_states["id"] = df_table_states.index + 1

state_sql = """
   INSERT INTO estado (codigo, nome)
   VALUES (%(codigo)s, %(nome)s);
"""

for index, row in df_table_states.iterrows():
    row_dict = {'codigo': row['id'], 'nome': row["state"]}
    insert_data(state_sql, row_dict)
   
#%% Filtro pelas colunas que serão necessárias na Tabela Cidade, dropando as duplicidades e fazendo a inserção
cols_cidades = ['city_ibge_code', 'city', 'state', 'estimated_population_2019']
df_table_cidades = df_selected_cities.filter(cols_cidades, axis=1).drop_duplicates()

# Criando a coluna state_id com um valor padrão
df_table_cidades['state_id'] = 0

# Atualizando a coluna state_id de acordo com o dataframe de estados
for index, row in df_table_cidades.iterrows():
    df_table_cidades.loc[index, 'state_id'] = df_table_states[df_table_states['state'] == row["state"]]['id'].item()

city_sql = """
   INSERT INTO cidade (codigo_ibge, nome, populacao_estimada, estado_id)
   VALUES (%(codigo_ibge)s, %(nome)s, %(populacao_estimada)s, %(estado_id)s);
"""

for index, row in df_table_cidades.iterrows():
    row_dict = {
        'codigo_ibge': row['city_ibge_code'], 
        'nome': row["city"],
        'populacao_estimada': row["estimated_population_2019"],
        'estado_id': row["state_id"],
    }
    insert_data(city_sql, row_dict)

#%% Filtro pelas colunas que serão necessárias na Tabela Resumo e inserção
cols_resumo = ['date', 'last_available_confirmed', 'last_available_deaths', 'city_ibge_code']
df_table_cidades = df_selected_cities.filter(cols_resumo, axis=1)

resumo_sql = """
   INSERT INTO resumo (data, ultimos_confirmados, ultimas_mortes, cidade_id)
   VALUES (%(data)s, %(ultimos_confirmados)s, %(ultimas_mortes)s, %(cidade_id)s);
"""

for index, row in df_table_cidades.iterrows():
    row_dict = {
        'data': row['date'], 
        'ultimos_confirmados': row["last_available_confirmed"],
        'ultimas_mortes': row["last_available_deaths"],
        'cidade_id': row["city_ibge_code"],
    }
    insert_data(resumo_sql, row_dict)