# -*- coding: utf-8 -*-
"""
@authors: Patrick Lima e Leonardo Gonçalves
"""

#%% Importar os pacotes de conexão ao BD
import psycopg2
import pandas as pd

FS = (16, 8)  # figure size

#%% Credenciais do Banco de Dados
user = "postgres"
password = "4527"
dbname = "covid"

#%% Conexão no Banco de Dados
conn = psycopg2.connect(user=user, password=password, dbname = dbname)
cur = conn.cursor()

#%% Função helper para retornar os nomes das colunas por Tabela

def get_column_names(table):
    cur.execute(
        f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}';")
    col_names = [result[0] for result in cur.fetchall()]
    return col_names

#%% Função helper para retornar conforme uma query, colunas e um dataframe pre-definido

def get_data_from_db(query, df, col_names):
    try:
        cur.execute(query)
        while True:
            # Fetch the next 100 rows
            query_results = cur.fetchmany(100)
            # If an empty list is returned, then we've reached the end of the results
            if query_results == list():
                break

            # Create a list of dictionaries where each dictionary represents a single row
            results_mapped = [
                {col_names[i]: row[i] for i in range(len(col_names))}
                for row in query_results
            ]

            # Append the fetched rows to the DataFrame
            df = df.append(results_mapped, ignore_index=True)

        return df

    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        print("Query: " + str(cur.query))
        conn.rollback()
        
#%% Seleção das colunas das tabelas necessárias
col_names_city = get_column_names("cidade")
col_names_resumo = get_column_names("resumo")

print(col_names_city)

#%% Criando um DF vazio + fazendo seleção das colunas e informações que fazem o append no DF
query_df = pd.DataFrame()
query = """
    SELECT c.*, e.nome as state_name, r.*
    FROM cidade c 
    INNER JOIN estado e ON c.estado_id = e.codigo
    INNER JOIN resumo r ON r.cidade_id = c.codigo_ibge;
"""

cols_with_joins = col_names_city + ['state_name'] + col_names_resumo
query_df = get_data_from_db(query, query_df, cols_with_joins)

#%% Filtrando pelas 5 cidades selecionadas
query_df_city = pd.DataFrame()
query = """
    SELECT c.nome
    FROM cidade c
"""

query_df_city = get_data_from_db(query, query_df_city, ['nome'])

#%% Filtrando pela cidade e formatando a primeira coluna para a Data facilitando o plot
def filter_and_format_by_city(city_name):
    df_city_filter = query_df[query_df['nome'] == city_name]

    df_city_filter.data = pd.to_datetime(df_city_filter.data, format="%Y-%m-%d")
    df_city_filter.set_index("data", inplace=True)
    df_city_filter.index.name = "Data"
    return df_city_filter

#%% Construção do gráfico diário com o cálculo das ultimas mortes e confirmados dividido pela população a cada 100000 habitantes

def plot_df_by_last_case_and_deaths_by_100000_habitants(df, city_name):
    ax = (
        (100000 * df["ultimos_confirmados"] / df["populacao_estimada"])
        .rolling(7, center=True)
        .mean()
        .plot(style="-", figsize=FS, logy=True, alpha=0.6)
    )
    ax = (
        (100000 * df["ultimas_mortes"] / df["populacao_estimada"])
        .rolling(7, center=True)
        .mean()
        .plot(style="-", ax=ax, logy=True, alpha=0.6)
    )
    ax.grid()
    _ = ax.set(
        title=f"COVID-19 diário (por 100.000 habitantes) na cidade de {city_name} (escala logarítmica)",
        ylabel="Escala em Log",
    )
    _ = ax.legend(["Últimos casos confirmados diários", "Últimas mortes diárias"])
    ax.autoscale(enable=True, axis="x", tight=True)

#%% Filtro, otimização e plot dos valores na cidade de Araucária

first_city = query_df_city.iat[0, 0]
df_first = filter_and_format_by_city(first_city)
plot_df_by_last_case_and_deaths_by_100000_habitants(df_first, first_city)

#%% Filtro, otimização e plot dos valores na cidade de Quitandinha

second_city = query_df_city.iat[1, 0]
df_second = filter_and_format_by_city(second_city)
plot_df_by_last_case_and_deaths_by_100000_habitants(df_second, second_city)

#%% Filtro, otimização e plot dos valores na cidade de Uberlândia

third_cirty = query_df_city.iat[2, 0]
df_third = filter_and_format_by_city(third_cirty)
plot_df_by_last_case_and_deaths_by_100000_habitants(df_third, third_cirty)

#%% Filtro, otimização e plot dos valores na cidade de Canoinhas

fourth_city = query_df_city.iat[3, 0]
df_fourth = filter_and_format_by_city(fourth_city)
plot_df_by_last_case_and_deaths_by_100000_habitants(df_fourth, fourth_city)


#%% Filtro, otimização e plot dos valores na cidade de Gramado

fifth_city = query_df_city.iat[4, 0]
df_fifth = filter_and_format_by_city(fifth_city)
plot_df_by_last_case_and_deaths_by_100000_habitants(df_fifth, fifth_city)


#%% Otimização por Data de todos os valores
query_df_new = query_df
query_df_new.data = pd.to_datetime(query_df_new.data, format="%Y-%m-%d")
query_df_new.set_index("data", inplace=True)
query_df_new.index.name = "Data"
print(query_df)

#%% Cálculo das ultimas mortes por população com todos os valores da otimização a cada 100000 habitantes

covid_rate_all_cities = query_df_new[["nome", "ultimas_mortes", "populacao_estimada"]]
covid_rate_all_cities["covid_rate"] = 100000 * covid_rate_all_cities['ultimas_mortes'] / covid_rate_all_cities["populacao_estimada"]
covid_rate_all_cities.drop(["populacao_estimada", "ultimas_mortes"], axis=1, inplace=True)

covid_rate_all_cities = covid_rate_all_cities.pivot_table(index="Data", columns="nome", values="covid_rate")

#%% Plot agregado das cidades
ax = covid_rate_all_cities.rolling(7, center=True).mean().plot(figsize=FS, alpha=0.6)
ax.grid()
_ = ax.set(
    title="COVID-19 diário (por 100.000) nos Munícipios escolhidos",
    ylabel="Total de mortes COVID-19",
)
ax.autoscale(enable=True, axis="x", tight=True)


#%%

import matplotlib.pyplot as plt
import numpy as np

rotulos = ['Q1', 'Q2', 'Q3', 'Q4']
vendas_2018 = [2.0, 3.4, 3.0, 3.5]
vendas_2019 = [2.5, 3.2, 3.4, 2.0]
espessura = 0.35
x = np.arange(len(rotulos))

fig, ax = plt.subplots(figsize=(12, 8))

rects1 = ax.bar(x - espessura / 2, vendas_2018, espessura, label='2018', color='red')
rects2 = ax.bar(x + espessura / 2, vendas_2019, espessura, label='2019', color='green')

ax.set_ylabel('Milhões')
ax.set_xlabel('Quadrimestre')
ax.set_title('Vendas por quadrimestre/ano (em milhões)')
ax.set_xticks(x)
ax.set_xticklabels(rotulos)
ax.legend()

fig.tight_layout()

plt.show()



