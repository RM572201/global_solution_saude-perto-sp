# Saúde Perto SP

Projeto acadêmico (FIAP – Global Solution) de análise territorial das Unidades Básicas de Saúde (UBS) do Município de São Paulo.

## Objetivo

Integrar o cadastro das UBS, suas áreas de abrangência e dados estimados do Censo 2022 agregados por área de abrangência, produzindo indicadores por UBS para apoiar a análise do território atendido por cada unidade.

O projeto utiliza uma arquitetura de dados em camadas (Bronze, Silver e Gold) no Databricks:

- **Bronze:** arquivos de origem preservados no Volume `/Volumes/workspace/default/ssp_raw_files`.
- **Silver:** dados tratados e padronizados em tabelas Delta no Unity Catalog.
- **Gold:** tabela `workspace.default.ssp_gold_ubs_indicadores`, com uma linha por UBS (CNES), para consumo analítico.

Os dados não são armazenados neste repositório. Arquivos de origem permanecem no Volume e as tabelas permanecem no Unity Catalog.

## Estrutura de diretórios

```text
.
├── .github/
│   └── workflows/          # Automações de CI/CD (futuro)
├── app/
│   ├── frontend/           # Aplicação - interface (a definir)
│   └── backend/            # Aplicação - serviços (a definir)
├── src/
│   ├── ingestion/          # Ingestão de dados
│   ├── transformation/     # Transformações e regras de tratamento
│   ├── integration/        # Integração entre fontes
│   ├── pipelines/          # Orquestração dos processos
│   └── utils/              # Funções auxiliares compartilhadas
├── notebooks/              # Notebooks Databricks do projeto
├── tests/
│   ├── ingestion/
│   ├── transformation/
│   └── indicators/
├── config/                 # Configurações não sensíveis
├── docs/
│   ├── arquitetura/
│   └── dicionario_dados/
├── scripts/                # Scripts utilitários
├── .gitignore
├── requirements.txt
└── README.md
```
