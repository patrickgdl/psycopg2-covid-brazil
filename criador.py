# -*- coding: utf-8 -*-
"""
@author: Patrick Lima e Leonardo Gonçalves
"""
#%% Importar os pacotes de conexão ao BD
import psycopg2

#%% Credenciais do Banco de Dados
user = "postgres"
password = "your_password"

#%% Conexão ao PostgreSQL
psql_connection_string = 'user={} password={}'.format(user, password)
conn = psycopg2.connect(psql_connection_string)
cur = conn.cursor()

#%% Criação do Banco de Dados
conn.autocommit = True
sql_query = 'CREATE DATABASE grupo_gamma'

try:
    cur.execute(sql_query)
except Exception as e:
    print(f"{type(e).__name__}: {e}")
    print("Query: " + str(cur.query))
    cur.close()
else:
    # Revert autocommit
    conn.autocommit = False

#%% Função de criação de tabelas
conn_with_db = 'user={} password={} dbname=grupo_gamma'.format(user, password)
conn2 = psycopg2.connect(conn_with_db)
cur2 = conn.cursor()

def create_table(sql_query):
    try:
        cur2.execute(sql_query)
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        print("Query: " + str(cur2.query))
        conn2.rollback()
        cur2.close()
    else:
        conn2.commit()

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