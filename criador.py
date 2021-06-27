# -*- coding: utf-8 -*-
"""
@authors: Patrick Lima e Leonardo Gonçalves
"""

#%% Importar os pacotes de conexão ao BD
import psycopg2

#%% Credenciais do Banco de Dados
user = "postgres"
password = "your_password"

#%% Criação do Banco de Dados
conn = psycopg2.connect(user=user, password=password)
cur = conn.cursor()

sql_query = 'CREATE DATABASE grupo_gamma'

conn.autocommit = True
try:
    cur.execute(sql_query)
except Exception as e:
    print(f"{type(e).__name__}: {e}")
    print("Query: " + str(cur.query))
    cur.close()
else:
    conn.autocommit = False
    cur.close()
    conn.close()

#%% Função de criação de tabelas
conn = psycopg2.connect(
    user=user,
    password=password,
    dbname='grupo_gamma',
)
cur = conn.cursor()

def create_table(sql_query):
    try:
        cur.execute(sql_query)
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        print("Query: " + str(cur.query))
        conn.rollback()
        cur.close()
    else:
        conn.commit()

#%% Criando a tabela "estado"
state_sql = """
    CREATE TABLE estado (
        codigo INTEGER PRIMARY KEY,
        nome VARCHAR(2) UNIQUE NOT NULL
    )
"""
create_table(state_sql)


#%% Criando a tabela "cidade"
city_sql = """
    CREATE TABLE cidade (
        codigo_ibge INTEGER PRIMARY KEY,
        nome VARCHAR(100) UNIQUE NOT NULL,
        populacao_estimada INTEGER NOT NULL,
        estado_id INTEGER REFERENCES estado(codigo)
    )
"""
create_table(city_sql)

#%% Criando a tabela "resumo"
city_sql = """
    CREATE TABLE resumo (
        codigo SERIAL PRIMARY KEY,
        data date NOT NULL,
        ultimos_confirmados INTEGER NOT NULL,
        ultimas_mortes INTEGER NOT NULL,
        cidade_id INTEGER REFERENCES cidade(codigo_ibge)
    )
"""
create_table(city_sql)