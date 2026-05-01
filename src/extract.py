# Módulo Extract
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaFileUpload


import io
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CREDENTIALS_FILE = 'credentials/service_account.json'
SCOPES = ['https://www.googleapis.com/auth/drive']

def get_drive_service():
    try:
        credentials = Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES)
        return build('drive', 'v3', credentials=credentials)
    except Exception as e:
        logging.error(f"Erro de autenticação com o Google Drive: {e}")
        raise

def list_landing_files(service, folder_id):
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()
        files = results.get('files', [])
        logging.info(f"Arquivos encontrados na landing folder: {files}")
        return files
    except Exception as e:
        logging.error(f"Erro ao listar arquivos na landing folder: {e}")
        raise

def download_and_convert(service, file_id, file_name, mime_type, local_raw_path):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name, _ = os.path.splitext(file_name)
    new_file_name = f"{base_name}_{timestamp}.csv"
    local_path = os.path.join(local_raw_path, new_file_name)

    if mime_type == 'application/vnd.google-apps.spreadsheet':
        request = service.files().export_media(fileId=file_id, mimeType='text/csv')
        with open(local_path, 'wb') as fh:
            fh.write(request.execute())
        logging.info(f"Arquivo baixado: {new_file_name} em {local_raw_path}")
        return local_path
    else:
        # 1. Faz o pedido de download do arquivo genérico
        request = service.files().get_media(fileId=file_id)
        
        # 2. Cria um espaço vazio na memória para os bytes
        fh = io.BytesIO()
        
        # 3. Conecta o pedido de download com o espaço na memória
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        
        # 4. Baixa os pedacinhos do arquivo até terminar
        while done is False:
            status, done = downloader.next_chunk()
            
        # 5. Volta o "cursor" para o começo da memória para podermos ler
        fh.seek(0)
        
        # 6. Verifica se é um Excel. Se for, usa o Pandas para ler os bytes e salvar como CSV
        if mime_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or file_name.endswith('.xlsx'):
            df = pd.read_excel(fh)
            df.to_csv(local_path, index=False)
            logging.info(f"Excel convertido e salvo: {new_file_name}")
            
        # 7. Se já for um CSV, só salva os bytes diretos no nosso arquivo local
        elif mime_type == 'text/csv' or file_name.endswith('.csv'):
            with open(local_path, 'wb') as f:
                f.write(fh.read())
            logging.info(f"CSV salvo: {new_file_name}")
            
        else:
            logging.warning(f"Formato não suportado: {file_name}")
            return None
            
        return local_path

def upload_to_raw(service, local_path, file_name, raw_folder_id):
    try:
        file_metadata = {
            'name': file_name,
            'parents': [raw_folder_id]
        }
        # Prepara o arquivo para upload
        media = MediaFileUpload(local_path, mimetype='text/csv')
        
        # Executa o upload e pega o ID gerado
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        logging.info(f"Upload para Raw concluído! ID: {file.get('id')}")
        
        return file.get('id')
    except Exception as e:
        logging.error(f"Erro ao fazer upload para a Raw Zone ({file_name}): {e}")
        raise

def move_to_archive(service, file_id, archive_folder_id):
    try:
        # 1. Pega os "pais" (pastas) atuais do arquivo
        file = service.files().get(fileId=file_id, fields='parents').execute()
        previous_parents = ",".join(file.get('parents'))
        
        # 2. Faz o update trocando os pais
        service.files().update(
            fileId=file_id,
            addParents=archive_folder_id,
            removeParents=previous_parents,
            fields='id, parents'
        ).execute()
        
        logging.info(f"Arquivo movido para a pasta Archive com sucesso!")
    except Exception as e:
        logging.error(f"Erro ao mover arquivo ({file_id}) para Archive: {e}")
        raise

def run_extraction(landing_folder_id, raw_folder_id, archive_folder_id, local_raw_path='./data/raw'):
    """Executa o pipeline completo de extração arquivo por arquivo."""
    
    # Garante que a pasta local "./data/raw" exista no nosso computador
    os.makedirs(local_raw_path, exist_ok=True)
    
    # Pega o "crachá" de acesso do Google
    service = get_drive_service()
    
    logging.info("Iniciando varredura na Landing Zone...")
    files = list_landing_files(service, landing_folder_id)
    
    if not files:
        logging.info("Nenhum arquivo novo encontrado na Landing Zone.")
        return 0
        
    logging.info(f"Encontrados {len(files)} arquivo(s). Iniciando processamento...")
    processed_count = 0
    
    for file in files:
        file_id = file.get('id')
        file_name = file.get('name')
        mime_type = file.get('mimeType')
        logging.info(f"Processando: {file_name}...")
        
        # 1. Baixar e Converter (e pegar o caminho final local)
        local_path = download_and_convert(service, file_id, file_name, mime_type, local_raw_path)
        
        if local_path:
            # Pegando apenas o nome do arquivo, sem o caminho inteiro da pasta
            new_file_name = os.path.basename(local_path)
            
            # # 2. Fazer Upload para a Raw Zone (Backup)
            # upload_to_raw(service, local_path, new_file_name, raw_folder_id)
            
            # 3. Mover o original para o Archive
            move_to_archive(service, file_id, archive_folder_id)
            
            processed_count += 1
            
    logging.info(f"Extração concluída. {processed_count} arquivo(s) processado(s) com sucesso.")
    return processed_count
