import duckdb
import os
import pandas as pd
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

columns_to_keep = [
    'cod_atendimento',
    'data_do_agendamento',
    'profissional',
    'paciente',
    'idade',
    'como_conheceu',
    'id_amigo',
    'tipo_do_item',
    'qtd_item',
    'item',
    'status_do_agendamento',
    'forma_de_pagamento',
    'valor'
]

text_columns_to_standardize = [
    'profissional',
    'status_do_agendamento',
    'paciente',
    'tipo_do_item',
    'item',
    'como_conheceu'
]

columns_to_standardize_na = ['tipo_do_item', 'item', 'como_conheceu', 'idade']

currency_column = 'valor'
age_column = 'idade'

def transform_data_to_df(local_raw_path='./data/raw'):
    csv_pattern = os.path.join(local_raw_path, "*.csv")
    
    if not os.path.exists(local_raw_path) or not any(f.endswith('.csv') for f in os.listdir(local_raw_path)):
        logging.info("Nenhum CSV encontrado para transformação.")
        return pd.DataFrame()
        
    logging.info(f"Unindo todos os arquivos CSV de: {csv_pattern}...")
    
    try:
        # O DuckDB faz a união de todos os CSVs sozinho numa tacada só!
        query = f"SELECT * FROM read_csv_auto('{csv_pattern}', union_by_name=True)"
        df_final = duckdb.sql(query).df()
        
        logging.info(f"Transformação concluída! {len(df_final)} linhas.")
        return df_final
    except Exception as e:
        logging.error(f"Erro no DuckDB: {e}")
        raise

def check_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    df.drop_duplicates(inplace=True)
    logging.info(f"Linhas duplicadas removidas. Linhas restantes: {len(df)}")
    return df

def select_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    df = df[columns].copy()
    logging.info(f"Colunas selecionadas mantidas: {columns}")
    return df

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.normalize('NFKD')
    df.columns = df.columns.str.encode('ascii', errors='ignore')
    df.columns = df.columns.str.decode('utf-8')
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('.', '')
    df.columns = df.columns.str.lower()

    logging.info(f"Colunas renomeadas.")
    return df

def standardize_text_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    for column in columns:
        df[column] = df[column].str.title()
    logging.info(f"Colunas {columns} padronizadas.")
    return df

def trasform_currency_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    df[column_name] = df[column_name].astype(str).str.replace(',', '.').astype(float)
    logging.info(f"Coluna {column_name} convertida para float.")
    return df

def transform_age_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    df[column_name] = df[column_name].str.extract(r'(\d+)').astype(float).astype('Int64')
    logging.info(f"Coluna {column_name} convertida para int (suportando vazios).")
    return df

def standardize_na_values(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    for column in columns:
        df[column] = df[column].astype(str).str.strip()
        df[column] = df[column].replace(['-', '', '<NA>', 'nan', 'NaN', 'None'], 'Não Informado')
    logging.info(f"Valores NA padronizados: {columns}")
    return df

def remove_accents_from_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df[column] = df[column].astype(str).str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    df[column] = df[column].str.title()
    logging.info(f"Acentos removidos e texto padronizado na coluna {column}.")
    return df

def create_receipt_type_column(df: pd.DataFrame) -> pd.DataFrame:
    df['tipo_recebimento'] = 'A Receber'
    
    mask = df['forma_de_pagamento'].str.contains('Dinheiro|Pix|Ted|Debito', case=False, na=False)
    df.loc[mask, 'tipo_recebimento'] = 'Recebido'
    
    logging.info("Coluna 'tipo_recebimento' (Recebido vs A Receber) criada com sucesso.")
    return df

def data_transformations():
    logging.info("Iniciando transformação dos dados...")

    df = transform_data_to_df()
    df = check_duplicates(df)
    df = rename_columns(df)
    df = select_columns(df, columns_to_keep)
    df = standardize_text_columns(df, text_columns_to_standardize)
    df = trasform_currency_column(df, currency_column)
    df = transform_age_column(df, age_column)
    df = standardize_na_values(df, columns_to_standardize_na)
    df = remove_accents_from_column(df, 'forma_de_pagamento')
    df = create_receipt_type_column(df)

    logging.info(f"Transformação concluída! {len(df)} linhas.")
    return df