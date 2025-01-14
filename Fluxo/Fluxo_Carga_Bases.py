import pandas as pd
from pathlib import Path
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.types import Float
import unicodedata
from datetime import datetime
import time
import pdb


def main():

        #caminho do repositório
        bases_analfabetismo = Path(r'C:\Users\Cristiano\Curso_GitHub\Serasa\Bases\Despesa Educacao')

        #listar arquivos do diretório
        
        for item in bases_analfabetismo.rglob('*'):
            print(f"Arquivo:{item.name}")

            #Removido as bases pontuais devido possuierem uma estrutura diferente
            if item.name.__contains__('.csv') and item.parts[6] != "Bases Pontuais":
                 
                #lendo arquivo
                df = pd.read_csv(item, sep=';',  skiprows= 1, encoding='UTF-8', usecols= lambda column: not column.startswith ("Unnamed"))

                #deixar as colunas minusculas
                df.columns = df.columns.map(lambda x: x.lower())

                #Identificando colunas que representam anos(numéricas)
                colunas_numericas = [col for col in df.columns if col.isdigit()]
                print(colunas_numericas)

                #Identificando colunas que não são digitos
                colunas_string = [col for col in df.columns if not col.isdigit()]
                print(colunas_string)

                #Reorganizando os dados no formato tidy
                df_ajustado = pd.melt(
                    df,
                    #colunas que se repetem
                    id_vars= colunas_string,
                    #colunas que viram linhas
                    value_vars = colunas_numericas,
                    #nome da nova coluna
                    var_name="ano",
                    value_name="valor"
                )

                #adicionando a data da carga
                df_ajustado['data_carga'] = datetime.today().strftime('%Y-%m-%d')

                df_ajustado['valor'] = df_ajustado['valor'].str.replace(',','.').astype(float) if df_ajustado['valor'].dtypes == 'object' else df_ajustado['valor'].astype(float) 

                #Removendo Acentuação
                df_ajustado.columns = [remover_acentos(col) for col in df_ajustado.columns]

                carga_base_sql(df_ajustado,item.name.replace('.csv',''),"BD_SERASA")


#remover acentuação 
def remover_acentos(texto):
    return ''.join(
            c for c in unicodedata.normalize("NFD",texto)
            if unicodedata.category(c)!= 'Mn'

    )

#data_frame = base de dados
#nm_tb = nome da tabela
#nm_bd = nome do banco de dados
def carga_base_sql(data_frame,nm_tb,nm_bd):

    dt_inicio_carga = datetime.now()

    # Definir a string de conexão
    conn_str = f'mssql+pyodbc://DESKTOP-H906D1Q/{nm_bd}?driver=ODBC+Driver+17+for+SQL+Server'

    # Criar a engine de conexão usando SQLAlchemy
    engine = create_engine(conn_str)

    #iniciando instância sql
    cursor = retornar_conexao_sql("BD_SERASA")

    #Tratamento de erro na importação
    try:

        #Inserir o DataFrame no banco de dados
        data_frame.to_sql(nm_tb, con=engine, if_exists='replace', index=False)

        #Registra log de execução
        cursor.execute ('INSERT INTO dbo.Tb_Log_Carga_Bases (dt_inicio_carga,dt_fim_carga, tempo_execucao, nm_base,resultado) VALUES (?,?,?,?,?);',
                        
                        (dt_inicio_carga,datetime.now(),str(datetime.now() - dt_inicio_carga).split('.')[0], nm_tb,'Sucesso')
                        )
        cursor.commit()
        cursor.close()
    except Exception as erro:
         #Registra log de execução
        cursor.execute ('INSERT INTO dbo.Tb_Log_Carga_Bases (dt_inicio_carga, dt_fim_carga,tempo_execucao,nm_base,resultado, mensagem) VALUES (?,?,?,?,?,?) ;',
                        
                        (dt_inicio_carga,datetime.now(), str(datetime.now() - dt_inicio_carga).split('.')[0],nm_tb,"Erro", erro)
                        )
        cursor.commit()
        cursor.close()

#Conexão Instrução Sql
#Nome do Banco de Dados
def retornar_conexao_sql(nm_bd):
    server = "DESKTOP-H906D1Q"
    database = nm_bd
    #username = "aula_mongodb"
    #password = "abc123"
    #string_conexao = 'Driver={SQL Server Native Client 11.0};Server='+server+';Database='+database+';UID='+username+';PWD='+ password
    string_conexao = 'Driver={ODBC Driver 17 for SQL Server};Server='+server+';Database='+database+';Trusted_Connection=yes;'
    conexao = pyodbc.connect(string_conexao)
    return conexao.cursor()


#Base Pontual população 2019 até 2021
#No sql calcular estimativa para 2023
def base_pontual_pop():

    df = pd.read_csv(r"C:\Users\Cristiano\Curso_GitHub\Serasa\Bases\Bases Pontuais\Tb_Populacao_2019_2021.csv", sep=',')

    #renomeando a coluna
    #inplace = true faz a mudança direto no frame
    df.rename(columns={'sigla_uf':'sigla','populacao':'valor'}, inplace=True)    

    #removendo coluna
    df.drop(['populacao_economicamente_ativa'],axis='columns', inplace=True)

    #Input da base populacional 2019 até 2021
    carga_base_sql(df,"Tb_Populacao_Ibge","BD_SERASA")

#Base Pontual com de_para Sigla, UF e Regiao
def base_pontual_uf():

    df = pd.read_excel(r"C:\Users\Cristiano\Curso_GitHub\Serasa\Bases\Pib\Tb_Pib_Per_Capita.xlsx", sheet_name='Planilha1')

    #deixar as colunas minusculas
    df.columns = df.columns.map(lambda x: x.lower())

    #Removendo Acentuação
    df.columns = [remover_acentos(col) for col in df.columns]

    print(df.head())

    #Input da base populacional 2019 até 2021
    carga_base_sql(df,"Tb_Pib_Per_Capita","BD_SERASA")


if __name__ == "__main__":
    main()   