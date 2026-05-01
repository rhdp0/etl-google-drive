from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import logging
import pandas as pd

# O escopo agora é de planilhas!
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CREDENTIALS_FILE = 'credentials/service_account.json'

def get_sheets_service():
    """Autentica e retorna o serviço do Google Sheets."""
    try:
        credentials = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        return build('sheets', 'v4', credentials=credentials)
    except Exception as e:
        logging.error(f"Erro de autenticação com o Google Sheets: {e}")
        raise

def get_historical_data(service, sheet_id):
    """Lê os dados existentes na planilha Trusted."""
    try:
        logging.info("Baixando histórico da Trusted Zone para evitar duplicações...")
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range='A1:ZZ',
            valueRenderOption='UNFORMATTED_VALUE'
        ).execute()
        
        values = result.get('values', [])
        if not values or len(values) < 2:
            return pd.DataFrame()
            
        columns = values[0]
        # Garantir que todas as linhas tenham a mesma quantidade de colunas do cabeçalho
        data = [row + [''] * (len(columns) - len(row)) for row in values[1:]]
        
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        logging.warning(f"Histórico não encontrado (Pode ser a primeira carga): {e}")
        return pd.DataFrame()

def upload_to_trusted(df, sheet_id):
    """Envia o DataFrame para a planilha Trusted de forma Idempotente (Read-Merge-Drop-Replace)."""
    if df is None or df.empty:
        logging.info("DataFrame vazio, nada a carregar na Trusted Zone.")
        return

    service = get_sheets_service()
    
    try:
        historical_df = get_historical_data(service, sheet_id)
        
        # 1. Prepara os dados novos: converter datas para string
        cols_data = df.select_dtypes(include=['datetime64', 'datetimetz', '<M8[ns]']).columns
        for col in cols_data:
            df[col] = df[col].dt.strftime('%Y-%m-%d')
            
        # Converte para string para garantir comparação perfeita com o histórico que vem do Sheets
        df_new_str = df.fillna('').astype(str)
        
        # 2. Merge e Deduplicação
        if not historical_df.empty:
            df_hist_str = historical_df.fillna('').astype(str)
            # Concatena o antigo com o novo
            merged = pd.concat([df_hist_str, df_new_str], ignore_index=True)
            # Exclui linhas perfeitamente iguais mantendo apenas a última
            df_clean = merged.drop_duplicates(keep='last')
            logging.info(f"Merge Concluído: Histórico ({len(historical_df)}) + Novos ({len(df)}) -> Base Dedupada ({len(df_clean)} linhas).")
        else:
            df_clean = df_new_str
            logging.info(f"Carga Inicial: {len(df_clean)} linhas prontas para subir.")

        # 3. Limpar a planilha atual
        logging.info(f"Limpando a planilha Trusted para a substituição...")
        service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range='A1:ZZ' 
        ).execute()
        
        # 4. Transforma o DataFrame em Lista de Listas e sobe
        values = [df_clean.columns.values.tolist()] + df_clean.values.tolist()
        body = {'values': values}
        
        logging.info("Enviando dados consolidados para a nuvem...")
        result = service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range='A1',
            valueInputOption='RAW',
            body=body
        ).execute()
        
        logging.info(f"Carga concluída com sucesso! {result.get('updatedCells')} células atualizadas na Trusted.")
    except Exception as e:
        logging.error(f"Erro ao carregar dados na Trusted Zone: {e}")
        raise
