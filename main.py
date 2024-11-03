import pandas as pd
from sqlalchemy import create_engine

# Configurações do banco de dados
DB_TYPE = 'postgresql'
DB_DRIVER = 'psycopg2'
DB_USER = 'seu_usuario'
DB_PASSWORD = 'sua_senha'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'seu_banco_de_dados'

# Função de Extração
def extract_data(file_path):
    # Ler dados de um arquivo CSV
    df = pd.read_csv(file_path)
    return df

# Função de Transformação
def transform_data(df):
    # Exemplo de transformação: remover linhas com valores ausentes
    df = df.dropna()
    # Exemplo de transformação: converter uma coluna para o tipo correto
    df['data'] = pd.to_datetime(df['data'])
    return df

# Função de Carga
def load_data(df):
    # Criar uma conexão com o banco de dados
    engine = create_engine(f'{DB_TYPE}+{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    
    # Carregar os dados no banco de dados em uma tabela chamada 'tabela_destino'
    df.to_sql('tabela_destino', con=engine, if_exists='replace', index=False)
    print("Dados carregados com sucesso!")

# Pipeline ETL
def run_etl(file_path):
    print("Iniciando o ETL...")
    # Extração
    data = extract_data(file_path)
    print("Dados extraídos:")
    print(data.head())
    
    # Transformação
    transformed_data = transform_data(data)
    print("Dados transformados:")
    print(transformed_data.head())
    
    # Carga
    load_data(transformed_data)

# Executar o pipeline ETL
if __name__ == "__main__":
    file_path = 'caminho/para/seu/arquivo.csv'  # Substitua pelo caminho do seu arquivo CSV
    run_etl(file_path)
