# -*- coding: utf-8 -*-
"""
@authors: Patrick Lima e Leonardo Gonçalves
"""

#%% Importar os pacotes de conexão ao BD
import psycopg2
import pandas as pd

#%% Credenciais do Banco de Dados
user = "postgres"
password = "your_password"
dbname = 'grupo_gamma'

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
print(query_df)

#%%
