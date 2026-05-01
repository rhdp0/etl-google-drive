import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import LANDING_FOLDER_ID, RAW_FOLDER_ID, ARCHIVE_FOLDER_ID, TRUSTED_SHEET_ID

from src.extract import run_extraction
from src.transform import data_transformations
from src.load import upload_to_trusted

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Iniciando o Pipeline ETL...")
    
    # Executando a extração passando os IDs
    arquivos_processados = run_extraction(
        landing_folder_id=LANDING_FOLDER_ID,
        raw_folder_id=RAW_FOLDER_ID,
        archive_folder_id=ARCHIVE_FOLDER_ID
    )
    
    logging.info(f"Extração concluída com {arquivos_processados} arquivo(s).")

    # Fase 2: Transformação de Dados (Transform)
    df = data_transformations()
    if df.empty:
        logging.info("O DataFrame está vazio. O pipeline não fará o Load e será encerrado com sucesso.")
        return
    
    logging.info("Iniciando Fase 3: Carga (Load)...")
    upload_to_trusted(df, TRUSTED_SHEET_ID)

    logging.info("Pipeline ETL finalizado com sucesso! Pode abrir seu Looker Studio!")

if __name__ == '__main__':
    main()