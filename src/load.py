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

def upload_to_trusted(df, sheet_id):
    """Envia o DataFrame para a planilha Trusted, sobrescrevendo tudo."""
    if df is None or df.empty:
        logging.info("DataFrame vazio, nada a carregar na Trusted Zone.")
        return

    service = get_sheets_service()
    
    try:
        # 1. Limpar a planilha atual para não acumular sujeira
        logging.info(f"Limpando a planilha Trusted...")
        service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range='A1:ZZ' 
        ).execute()

        # 2. Prepara os dados: APIs não gostam de "NaN" ou objetos de Data (Timestamp).
        # Primeiro convertemos as colunas de data para texto:
        cols_data = df.select_dtypes(include=['datetime64', 'datetimetz', '<M8[ns]']).columns
        for col in cols_data:
            df[col] = df[col].dt.strftime('%Y-%m-%d')
            
        # Depois convertemos tudo para 'object' (genérico) e preenchemos os vazios com string vazia
        df_clean = df.astype(object).fillna('')
        
        # 3. Transforma o DataFrame em uma "Lista de Listas" (com o cabeçalho no topo)
        values = [df_clean.columns.values.tolist()] + df_clean.values.tolist()
        body = {'values': values}
        
        # 4. Envia os novos dados
        logging.info("Enviando novos dados para a planilha...")
        result = service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range='A1',
            valueInputOption='RAW',
            body=body
        ).execute()
        
        logging.info(f"Carga concluída! {result.get('updatedCells')} células atualizadas na Trusted.")
    except Exception as e:
        logging.error(f"Erro ao carregar dados na Trusted Zone: {e}")
        raise
