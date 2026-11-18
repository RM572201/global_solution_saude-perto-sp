# Saúde Perto SP

Projeto acadêmico (FIAP – Global Solution) de análise territorial das Unidades Básicas de Saúde (UBS) do Município de São Paulo.

## Objetivo

Integrar o cadastro das UBS, suas áreas de abrangência e dados estimados do Censo 2022 agregados por área de abrangência, produzindo indicadores por UBS para apoiar a análise do território atendido por cada unidade.

O projeto utiliza uma arquitetura de dados em camadas (Bronze, Silver e Gold) no Databricks:

- **Bronze:** arquivos de origem preservados no Volume `/Volumes/workspace/default/ssp_raw_files`.
- **Silver:** dados tratados e padronizados em tabelas Delta no Unity Catalog.
- **Gold:** tabela `workspace.default.ssp_gold_ubs_indicadores`, com uma linha por UBS (CNES), para consumo analítico.

## Notebooks

| Notebook | Etapa | Saída |
| --- | --- | --- |
| `00_setup_ambiente` | Setup | Volume `ssp_raw_files` |
| `01_bronze_ingestao` | Bronze | `ssp_bronze_manifest` |
| `02_silver_ubs_cadastro` | Silver | `ssp_silver_ubs_cadastro` |
| `03_silver_areas_abrangencia` | Silver | `ssp_silver_areas_abrangencia` |
| `04_silver_censo` | Silver | `ssp_silver_censo` |
| `05_gold_ubs_indicadores` | Gold | `ssp_gold_ubs_indicadores` |
| `06_clusterizacao_ubs` | Análise (ML) | `ssp_gold_ubs_perfis`, view `vw_ssp_ubs_perfis_bi`, modelo `ssp_modelo_perfis_ubs` |
| `07_correlacao_associacao` | Análise estatística | `ssp_gold_associacoes` |
| `documentacao_modelo_silver` | Documentação | Modelo de dados Bronze/Silver |

Todas as tabelas, views e o modelo ficam em `workspace.default` (Unity Catalog).

### 06 - Clusterização das UBS

- KMeans (scikit-learn), k = 4, sobre 9 indicadores relativos da Gold (faixa etária, analfabetismo, raça/cor, unidades unipessoais, FCU, esgoto, densidade e renda). Renda, densidade e esgoto recebem `log1p`; todas as features são padronizadas dentro do `Pipeline`.
- Perfis: Urbano intermediário (212 UBS), Urbano vulnerável (favelas e comunidades) (143), Consolidado de alta renda (111) e Periferia de baixa densidade e sem saneamento (19). Silhouette 0,291; ARI entre seeds 0,967.
- Premissa: `P_V001AA_FCU` nulo (71 áreas sem registro de FCU) é tratado como 0% no modelo.
- Modelo registrado no Unity Catalog: `workspace.default.ssp_modelo_perfis_ubs` (versão vigente: 3, que gerou os perfis atuais de `ssp_gold_ubs_perfis`). As versões 1 a 3 vêm de execuções com os mesmos parâmetros e seeds, e os tamanhos dos perfis são idênticos. `predict()` retorna o id do cluster; o nome do perfil está em `ssp_gold_ubs_perfis`, cuja coluna `mlflow_run_id` indica qual run gerou a atribuição.
- Consumo em BI: `vw_ssp_ubs_perfis_bi` (1 linha por UBS, com perfil, indicadores, coordenadas e polígono WKT da área de abrangência).
- Reexecutar o notebook inteiro recria `ssp_gold_ubs_perfis` e registra uma nova versão do modelo.

### 07 - Correlação e associação

- Pearson e Spearman entre 10 indicadores; η² e Kruskal-Wallis dos indicadores por CRS e por perfil; qui-quadrado e V de Cramér para perfil × CRS.
- Resultados em formato longo em `ssp_gold_associacoes` (1 linha por tipo de análise, par de variáveis e medida).
- Os dados cobrem todas as UBS (não é amostra): os p-valores são referência, e a interpretação se baseia na força das relações.

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
