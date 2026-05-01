# 🏥 Data Engineering Pipeline: Medical Clinic Automation

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12-blue.svg" alt="Python">
  <img src="https://img.shields.io/badge/Pandas-Data_Processing-150458.svg" alt="Pandas">
  <img src="https://img.shields.io/badge/DuckDB-OLAP-FFC800.svg" alt="DuckDB">
  <img src="https://img.shields.io/badge/Google_API-Drive_%7C_Sheets-EA4335.svg" alt="Google APIs">
  <img src="https://img.shields.io/badge/GitHub_Actions-CI%2FCD-2088FF.svg" alt="GitHub Actions">
</p>

## 📌 Visão Geral
Este projeto é um pipeline de Engenharia de Dados **End-to-End** automatizado para uma clínica médica. O objetivo principal foi substituir um processo diário, manual e suscetível a erros, por uma orquestração em nuvem, garantindo governança e disponibilidade de dados higienizados (D-0) para um Dashboard no **Looker Studio**.

O pipeline consome planilhas financeiras e de agendamentos diretamente do Google Drive, aplica rigorosas regras de limpeza de dados e insere a base consolidada em uma "Trusted Zone" no Google Sheets.

## 🏗️ Arquitetura do Projeto

O fluxo de dados foi desenhado seguindo as melhores práticas de Engenharia de Dados (Medallion Architecture adaptada para o ecossistema G-Suite):

1. **Extract (Landing ➡️ Raw/Archive):**
   * Conexão via `Service Account` do Google Cloud Platform (GCP).
   * Varredura de novos arquivos na Landing Zone.
   * Download otimizado (em chunks) dos arquivos para a zona local `raw` e movimentação automática para o diretório `archive` na nuvem para evitar reprocessamento.

2. **Transform (DuckDB + Pandas):**
   * **Consolidação de Lotes:** Utilização do motor analítico super rápido do **DuckDB** para unir dezenas de arquivos locais simultaneamente via `read_csv_auto`.
   * **Whitelist de Colunas:** Filtro explícito das colunas de interesse, garantindo resiliência do código contra "Schema Drifts" (adição de colunas inesperadas pelo sistema fonte).
   * **Data Cleansing & Regras de Negócio:** 
     * Normalização de texto (Regex, `.str.title()`, ASCII).
     * Remoção de acentuação e padronização para evitar duplicidade de categorias no BI (Ex: "Saúde" ➡️ "Saude").
     * Casting robusto de Tipos (tratamento do ponto flutuante brasileiro e uso do `Int64` do Pandas para tratar inteiros contendo valores nulos `NaN`).
     * Criação inteligente de flags analíticas (Ex: `Recebido` vs `A Receber` com base em Regex nas formas de pagamento).

3. **Load (Trusted Zone):**
   * Serialização da base final com substituição das notações internas do Pandas (`<NA>`, `NaN`, `NaT`) e serialização de objetos `Timestamp` visando compatibilidade máxima com o encoder JSON da API do Google.
   * Operação `Drop & Replace` da base Trusted para alimentar os painéis executivos, limpando o histórico para não haver sobreposição suja.

4. **Orquestração (GitHub Actions):**
   * Execução diária (`cron`) da rotina utilizando Runners do Ubuntu no GitHub Actions. Variáveis de ambiente e chaves secretas (Service Accounts) injetados de maneira segura estritamente via **GitHub Secrets**.

## 📂 Estrutura de Diretórios

```bash
📦 etl-google-drive-v2
 ┣ 📂 .github/workflows
 ┃ ┗ 📜 main.yml            # Pipeline CI/CD de execução automática diária
 ┣ 📂 config
 ┃ ┗ 📜 settings.py         # Gerenciamento de variáveis de ambiente com python-decouple
 ┣ 📂 credentials           # Repositório de chaves do GCP (Ignorado no Git)
 ┣ 📂 data
 ┃ ┣ 📂 archive             # Backup dos lotes brutos pós-download
 ┃ ┗ 📂 raw                 # Landing zone local
 ┣ 📂 src
 ┃ ┣ 📜 extract.py          # Lógica de extração com Google Drive API
 ┃ ┣ 📜 transform.py        # Limpeza, Deduplicação, Tipagem e Regras de Negócio
 ┃ ┣ 📜 load.py             # Carga higienizada na Trusted Sheet (Google Sheets API)
 ┃ ┗ 📜 main.py             # Controlador Central (Orquestrador ETL)
 ┣ 📜 .env                  # Variáveis de IDs de pasta (Ignorado no Git)
 ┣ 📜 .gitignore            # Blindagem de segurança (Credenciais e Dados sensíveis)
 ┣ 📜 requirements.txt      # Dependências (Pandas, Duckdb, Google-API, etc.)
 ┗ 📜 README.md             # Documentação do Projeto
```

## 🚀 Como Executar Localmente

**1. Clone o repositório:**
```bash
git clone https://github.com/seu-usuario/etl-google-drive-v2.git
cd etl-google-drive-v2
```

**2. Instale as dependências em um ambiente virtual:**
```bash
python -m venv venv
source venv/bin/activate  # ou venv\Scripts\activate no Windows
pip install -r requirements.txt
```

**3. Configure as Variáveis:**
* Crie um arquivo `.env` na raiz do projeto contendo: `LANDING_FOLDER_ID`, `RAW_FOLDER_ID`, `ARCHIVE_FOLDER_ID`, `TRUSTED_SHEET_ID`.
* Coloque o seu `service_account.json` gerado pelo Google Cloud console dentro da pasta `credentials/`.

**4. Execute o Orquestrador:**
```bash
python src/main.py
```

## 💡 Habilidades e Ferramentas Demonstradas no Projeto
* **Linguagens e Ferramentas:** Python (Pandas), SQL (DuckDB).
* **Integração de APIs REST:** Autenticação OAuth2, paginação e permissões de Service Account no ecosistema Google Cloud Platform.
* **Segurança da Informação:** Configuração de `.gitignore`, uso de `.env` e CI/CD Secrets para blindagem de dados sensíveis e credenciais (Adequação LGPD).
* **Engenharia de Software:** Arquitetura modularizada em camadas (`src/`), componentização, rastreamento de logs (`logging`), resiliência no casting de dados e uso de Whitelist contra corrupção estrutural.

---
*Desenvolvido como projeto de automação corporativa e portfólio de Data Engineering.*
