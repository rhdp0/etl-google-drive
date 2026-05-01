# 🏥 Projeto ETL: Automação Clínica com "Zero Cost" Data Stack

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12-blue.svg" alt="Python">
  <img src="https://img.shields.io/badge/Pandas-Data_Processing-150458.svg" alt="Pandas">
  <img src="https://img.shields.io/badge/DuckDB-OLAP-FFC800.svg" alt="DuckDB">
  <img src="https://img.shields.io/badge/Google_API-Drive_%7C_Sheets-EA4335.svg" alt="Google APIs">
  <img src="https://img.shields.io/badge/GitHub_Actions-CI%2FCD-2088FF.svg" alt="GitHub Actions">
</p>

## 📌 Visão Geral
Este é um projeto prático que construí para aplicar conceitos reais de Engenharia de Dados resolvendo um problema do dia a dia: automatizar a leitura de planilhas de agendamento de uma clínica médica e alimentar um dashboard no **Data Studio**, tudo isso com uma arquitetura de baixo custo.

Em vez de subir ferramentas caras de nuvem para um volume de dados inicial que ainda não exige isso, utilizei a criatividade para **simular uma arquitetura moderna** usando o ecossistema gratuito do Google Workspace combinado com Python.

## 🏗️ Arquitetura do Projeto

O fluxo de dados foi desenhado imitando uma arquitetura *Medallion* (Landing, Raw, Trusted):

1. **Extract (Google Drive como Data Lake 🌊):**
   * A estrutura de pastas do Google Drive atua como nosso repositório de dados brutos.
   * O script se conecta via `Service Account` do GCP, verifica a pasta **Landing** procurando novos CSVs, faz o download para processamento local e move os originais para a pasta **Archive**.

2. **Transform (DuckDB + Pandas 🐍):**
   * **DuckDB:** Escolhido pelo seu motor analítico que une e lê múltiplos arquivos locais de forma muito veloz.
   * **Pandas:** Realiza as regras de negócio de fato:
     * *Whitelist* de colunas para garantir resiliência estrutural.
     * Padronização de strings (remoção total de acentos e capitalização para unificar as dimensões do BI).
     * Tipagem forte (tratando nulls do Pandas sem quebrar o envio pro JSON).
     * Criação inteligente de flags (Ex: `Recebido` vs `A Receber` com base em Regex na forma de pagamento).

3. **Load (Google Sheets como Data Mart 🏛️):**
   * Para evitar a duplicação de pacientes (garantir idempotência), criei a estratégia **Read-Merge-Drop-Replace**: O código baixa o histórico atual do Google Sheets, une com os novos dados processados, elimina duplicações usando `drop_duplicates` e devolve a "Única Fonte da Verdade" limpa para a planilha, de onde o Data Studio puxa os visuais.

4. **Orquestração (GitHub Actions 🤖):**
   * O pipeline roda automaticamente na nuvem todos os dias através de um `cron` no GitHub Actions. Chaves e permissões sensíveis não constam no código e são injetadas no ambiente via **GitHub Secrets**.

## 📂 Estrutura de Diretórios

```bash
📦 etl-google-drive-v2
 ┣ 📂 .github/workflows
 ┃ ┗ 📜 main.yml            # Pipeline CI/CD de execução automática diária
 ┣ 📂 config
 ┃ ┗ 📜 settings.py         # Gerenciamento de variáveis de ambiente (.env)
 ┣ 📂 credentials           # Repositório de chaves do GCP (Ignorado no Git)
 ┣ 📂 data
 ┃ ┣ 📂 archive             # Lotes processados
 ┃ ┗ 📂 raw                 # Landing zone local
 ┣ 📂 src
 ┃ ┣ 📜 extract.py          # Integração Google Drive API
 ┃ ┣ 📜 transform.py        # Limpeza, Deduplicação e Regras de Negócio
 ┃ ┣ 📜 load.py             # Lógica Idempotente para o Google Sheets API
 ┃ ┗ 📜 main.py             # Orquestrador Central
 ┣ 📜 .env                  # Ignorado no Git
 ┣ 📜 .gitignore            # Blindagem de dados brutos e senhas
 ┣ 📜 requirements.txt      # Dependências com versões "congeladas"
 ┗ 📜 README.md             
```

## 🚀 Como Executar Localmente

**1. Clone o repositório:**
```bash
git clone https://github.com/seu-usuario/etl-google-drive-v2.git
cd etl-google-drive-v2
```

**2. Ambiente virtual e Dependências:**
```bash
python -m venv venv
source venv/bin/activate  # ou venv\Scripts\activate no Windows
pip install -r requirements.txt
```

**3. Configurando as Variáveis:**
* Crie um `.env` com: `LANDING_FOLDER_ID`, `RAW_FOLDER_ID`, `ARCHIVE_FOLDER_ID`, `TRUSTED_SHEET_ID`.
* Salve sua chave `.json` do GCP na pasta `credentials/`.

**4. Execute:**
```bash
python src/main.py
```

## 💡 Próximos Passos (Roadmap)
O intuito desse projeto foi fixar os pilares de um pipeline ETL do zero. Como próximos passos para evoluir essa arquitetura, o roadmap planejado é migrar a infraestrutura simulada para ferramentas *Cloud Native*:
- **Storage:** Trocar o Google Drive por **Google Cloud Storage (GCS) / AWS S3**.
- **Data Warehouse:** Trocar o Google Sheets por **Google BigQuery**.
- **Orquestração:** Trocar o GitHub Actions por **Apache Airflow**.
