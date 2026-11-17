# Databricks notebook source
# DBTITLE 1,Documentação do modelo Silver
# MAGIC %md
# MAGIC # Documentação do modelo de dados Silver | Projeto Saúde Perto SP
# MAGIC
# MAGIC **Escopo:** arquitetura e engenharia de dados das camadas Bronze e Silver do projeto acadêmico FIAP *Saúde Perto SP* (análise territorial das UBS do Município de São Paulo). Este documento não trata de camada Gold, indicadores derivados ou dashboards.
# MAGIC
# MAGIC **Tabelas documentadas** (catálogo `workspace`, schema `default`):
# MAGIC * `ssp_silver_ubs_cadastro`
# MAGIC * `ssp_silver_areas_abrangencia`
# MAGIC * `ssp_silver_censo`
# MAGIC
# MAGIC **Origem das informações** (somente leitura, em 17/11/2026):
# MAGIC * Nomes, tipos, nulabilidade e comentários das colunas: `workspace.information_schema.columns`.
# MAGIC * Quantidade de registros: `SELECT count(*)` em cada tabela.
# MAGIC * Histórico de versões: `DESCRIBE HISTORY`.
# MAGIC * Regras de transformação e qualidade: código dos notebooks `02_silver_ubs_cadastro`, `03_silver_areas_abrangencia` e `04_silver_censo` (pasta `Saude_Perto_SP`).
# MAGIC
# MAGIC Este notebook contém apenas texto. Não executa consultas e não altera dados.
# MAGIC
# MAGIC **Seções**
# MAGIC 1. Visão geral da arquitetura
# MAGIC 2. Dicionário de dados
# MAGIC 3. Chaves e relacionamentos
# MAGIC    * 3.4 Diagrama entidade-relacionamento (modelo lógico)
# MAGIC 4. Regras de negócio e qualidade
# MAGIC 5. Linhagem dos dados
# MAGIC 6. Resultados e limitações conhecidas
# MAGIC 7. Exemplos de consultas

# COMMAND ----------

# DBTITLE 1,1. Visão geral da arquitetura
# MAGIC %md
# MAGIC ## 1. Visão geral da arquitetura
# MAGIC
# MAGIC O projeto segue a arquitetura em camadas (*medallion*) sobre Delta Lake e Unity Catalog, em Databricks Serverless.
# MAGIC
# MAGIC ### Camada Bronze: cópia fiel das fontes
# MAGIC * Os três arquivos originais são copiados **sem alteração** para o Volume gerenciado `workspace.default.ssp_raw_files`, um subdiretório por fonte (notebooks `00_setup_ambiente` e `01_bronze_ingestao`).
# MAGIC * A tabela `workspace.default.ssp_bronze_manifest` registra cada ingestão: `fonte`, `caminho_bronze`, `sha256`, `status` e `id_ingestao`.
# MAGIC * O SHA-256 identifica a versão de cada arquivo. Os notebooks Silver só leem o arquivo cujo hash confere com o manifesto.
# MAGIC
# MAGIC | Fonte (manifesto) | Arquivo na Bronze | SHA-256 |
# MAGIC | --- | --- | --- |
# MAGIC | `consulta_ubs_kml` | `consulta_ubs_kml/Consulta UBS.kml` | `5fbf2247…73430d1` |
# MAGIC | `aaubs_2026_kmz` | `aaubs_2026_kmz/Areas_de_Abrangencia_de_UBS_no_MSP_2026.kmz` | `3884c603…1331113e` |
# MAGIC | `censo2022_aaubs_xlsx` | `censo2022_aaubs_xlsx/Dados_Estimados_Agregados_por_setores_UniversoCenso2022_MSP_SC_AAUBS2026_rev29052026.xlsx` | `b5332669…5ef723` |
# MAGIC
# MAGIC ### Camada Silver: dados estruturados, padronizados e validados
# MAGIC * Cada fonte gera uma tabela Delta gerenciada, com colunas em *snake_case*, tipos explícitos e comentários no catálogo.
# MAGIC * Os valores de origem são preservados (colunas `*_original`, `*_texto`, `descricao_html_original`). As inconsistências ficam em colunas de status; **nada é corrigido nem inferido**.
# MAGIC * Toda linha carrega rastreabilidade: `fonte_arquivo`, `fonte_sha256`, `id_ingestao_bronze`, `id_execucao`, `data_processamento`.
# MAGIC * As validações são exibidas nos notebooks. Não existe tabela de qualidade separada.
# MAGIC
# MAGIC | Tabela Silver | Notebook | Fonte Bronze | Finalidade |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `ssp_silver_ubs_cadastro` | `02_silver_ubs_cadastro` | `consulta_ubs_kml` | Cadastro das UBS: identificação, endereço, contato, regionalização e localização (ponto) |
# MAGIC | `ssp_silver_areas_abrangencia` | `03_silver_areas_abrangencia` | `aaubs_2026_kmz` | Polígono oficial (2026) da área de abrangência de cada UBS, com validações espaciais |
# MAGIC | `ssp_silver_censo` | `04_silver_censo` | `censo2022_aaubs_xlsx` | Dados estimados do Censo 2022 por área de abrangência, em formato longo |

# COMMAND ----------

# DBTITLE 1,2. Dicionário de dados — visão geral
# MAGIC %md
# MAGIC ## 2. Dicionário de dados
# MAGIC
# MAGIC Nomes, tipos e nulabilidade foram lidos de `workspace.information_schema.columns`. A coluna **Descrição** reproduz o comentário gravado no catálogo. Todas as colunas têm comentário. Quando o comentário cita um nome como `Fone_1` ou `SubPref_`, esse é o nome do campo na fonte original.
# MAGIC
# MAGIC | Tabela | Finalidade | Granularidade | Registros | Colunas | Versão Delta |
# MAGIC | --- | --- | --- | --- | --- | --- |
# MAGIC | `ssp_silver_ubs_cadastro` | Cadastro das UBS (pontos do KML *Consulta UBS*) | 1 linha por UBS (`cnes`) | 485 | 47 | 1 |
# MAGIC | `ssp_silver_areas_abrangencia` | Áreas de abrangência oficiais 2026 (SIRGAS 2000) | 1 linha por área / UBS (`cnes`) | 485 | 37 | 1 |
# MAGIC | `ssp_silver_censo` | Censo 2022 estimado por área de abrangência | 1 linha por `cnes` × `aba_origem` × `variavel_origem` | 652.230 | 18 | 1 |
# MAGIC
# MAGIC Colunas obrigatórias (`NOT NULL`): `cnes` e `fonte_sha256` nas três tabelas; em `ssp_silver_censo`, também `aba_origem` e `variavel_origem`.

# COMMAND ----------

# DBTITLE 1,2.1 ssp_silver_ubs_cadastro
# MAGIC %md
# MAGIC ### 2.1 `workspace.default.ssp_silver_ubs_cadastro`
# MAGIC
# MAGIC * **Finalidade:** cadastro das unidades de saúde de referência das áreas de abrangência, a partir dos 485 pontos (`Point`) da pasta `Estabelecimento de Saúde Referencia da AA-UBS` do arquivo `Consulta UBS.kml`.
# MAGIC * **Granularidade:** uma linha por CNES.
# MAGIC * **Registros:** 485.
# MAGIC * **Comentário da tabela:** "Projeto Saúde Perto SP - Silver: cadastro das UBS (pontos do KML Consulta UBS). Chave: cnes."
# MAGIC
# MAGIC | # | Coluna | Tipo | Nulo | Descrição (comentário do catálogo) |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1 | `cnes` | `STRING` | Não | CNES normalizado (texto, 7 dígitos) - chave de integração |
# MAGIC | 2 | `cnes_original` | `STRING` | Sim | CNES como consta na descrição HTML |
# MAGIC | 3 | `status_cnes` | `STRING` | Sim | OK ou COMPLETADO_ZEROS |
# MAGIC | 4 | `nome_placemark` | `STRING` | Sim | Nome do placemark no KML (descritivo; não é chave) |
# MAGIC | 5 | `nome_divulgacao` | `STRING` | Sim | Nome_Divul (descritivo; não é chave) |
# MAGIC | 6 | `telefone_1` | `STRING` | Sim | Fone_1 |
# MAGIC | 7 | `telefone_2` | `STRING` | Sim | Fone_2 |
# MAGIC | 8 | `logradouro` | `STRING` | Sim | Logradouro |
# MAGIC | 9 | `numero_logradouro` | `STRING` | Sim | Numero_Log (texto; pode conter valores não numéricos) |
# MAGIC | 10 | `complemento` | `STRING` | Sim | Complement |
# MAGIC | 11 | `bairro` | `STRING` | Sim | Bairro |
# MAGIC | 12 | `cep` | `STRING` | Sim | CEP como na origem |
# MAGIC | 13 | `cep_formato_valido` | `BOOLEAN` | Sim | CEP no padrão 00000-000 |
# MAGIC | 14 | `observacao_endereco` | `STRING` | Sim | Obs_Endere |
# MAGIC | 15 | `horario_funcionamento` | `STRING` | Sim | Horario_Fu |
# MAGIC | 16 | `subprefeitura` | `STRING` | Sim | SubPref_ |
# MAGIC | 17 | `supervisao_tecnica_saude` | `STRING` | Sim | STS |
# MAGIC | 18 | `uvis` | `STRING` | Sim | UVIS |
# MAGIC | 19 | `coordenadoria_regional_saude` | `STRING` | Sim | CRS (Coordenadoria Regional de Saúde) |
# MAGIC | 20 | `geom_longitude` | `DOUBLE` | Sim | Longitude da geometria Point do KML |
# MAGIC | 21 | `geom_latitude` | `DOUBLE` | Sim | Latitude da geometria Point do KML |
# MAGIC | 22 | `geom_altitude` | `DOUBLE` | Sim | Altitude da geometria Point do KML |
# MAGIC | 23 | `geometria_wkt` | `STRING` | Sim | Geometria em WKT (lon lat), texto original das coordenadas |
# MAGIC | 24 | `crs_geometria` | `STRING` | Sim | Referência da geometria (EPSG:4326, padrão KML) |
# MAGIC | 25 | `longitude_atributo_texto` | `STRING` | Sim | LONSIRGAS como na origem |
# MAGIC | 26 | `latitude_atributo_texto` | `STRING` | Sim | LATSIRGAS como na origem |
# MAGIC | 27 | `longitude_atributo` | `DOUBLE` | Sim | LONSIRGAS convertido |
# MAGIC | 28 | `latitude_atributo` | `DOUBLE` | Sim | LATSIRGAS convertido |
# MAGIC | 29 | `status_conversao_coord_atributo` | `STRING` | Sim | Resultado da conversão de LONSIRGAS/LATSIRGAS |
# MAGIC | 30 | `crs_atributos` | `STRING` | Sim | Referência dos atributos (EPSG:4674, SIRGAS 2000) |
# MAGIC | 31 | `distancia_geom_atributo_m` | `DOUBLE` | Sim | Distância (haversine) entre geometria e atributos, em metros |
# MAGIC | 32 | `status_coordenada` | `STRING` | Sim | IDENTICA, PROXIMA, DIVERGENTE, DIVERGENTE_POSSIVEL_INVERSAO, SEM_GEOMETRIA ou SEM_ATRIBUTO |
# MAGIC | 33 | `geometria_fora_bbox` | `BOOLEAN` | Sim | Ponto fora do envelope aproximado do MSP |
# MAGIC | 34 | `atributos_extras_json` | `STRING` | Sim | Chaves da descrição não mapeadas (JSON) |
# MAGIC | 35 | `chaves_ausentes` | `ARRAY<STRING>` | Sim | Chaves esperadas ausentes na descrição |
# MAGIC | 36 | `chaves_duplicadas` | `ARRAY<STRING>` | Sim | Chaves repetidas na descrição |
# MAGIC | 37 | `placemark_indice` | `INT` | Sim | Posição do placemark na pasta do KML |
# MAGIC | 38 | `titulo_descricao` | `STRING` | Sim | Primeiro segmento da descrição HTML |
# MAGIC | 39 | `style_url` | `STRING` | Sim | styleUrl do placemark |
# MAGIC | 40 | `coordenadas_texto` | `STRING` | Sim | Texto original de `<coordinates>` |
# MAGIC | 41 | `descricao_html_original` | `STRING` | Sim | Descrição HTML original (rastreabilidade) |
# MAGIC | 42 | `kml_pasta` | `STRING` | Sim | Pasta do KML de origem |
# MAGIC | 43 | `fonte_arquivo` | `STRING` | Sim | Caminho do arquivo Bronze |
# MAGIC | 44 | `fonte_sha256` | `STRING` | Não | SHA-256 do arquivo Bronze processado |
# MAGIC | 45 | `id_ingestao_bronze` | `STRING` | Sim | id_ingestao do manifesto Bronze |
# MAGIC | 46 | `id_execucao` | `STRING` | Sim | Identificador desta execução do notebook 02 |
# MAGIC | 47 | `data_processamento` | `TIMESTAMP` | Sim | Data/hora do processamento (UTC) |

# COMMAND ----------

# DBTITLE 1,2.2 ssp_silver_areas_abrangencia
# MAGIC %md
# MAGIC ### 2.2 `workspace.default.ssp_silver_areas_abrangencia`
# MAGIC
# MAGIC * **Finalidade:** polígono oficial da área de abrangência de cada UBS (KMZ 2026), com área informada, área calculada e resultado das validações espaciais.
# MAGIC * **Granularidade:** uma linha por área de abrangência, identificada pelo CNES da UBS de referência.
# MAGIC * **Registros:** 485.
# MAGIC * **Comentário da tabela:** "Projeto Saúde Perto SP - Silver: áreas de abrangência das UBS (KMZ oficial 2026, SIRGAS 2000). Chave: cnes."
# MAGIC
# MAGIC | # | Coluna | Tipo | Nulo | Descrição (comentário do catálogo) |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1 | `cnes` | `STRING` | Não | CNES normalizado (texto, 7 dígitos) - chave de integração |
# MAGIC | 2 | `cnes_original` | `STRING` | Sim | CNES_UBS como na origem |
# MAGIC | 3 | `status_cnes` | `STRING` | Sim | OK ou COMPLETADO_ZEROS |
# MAGIC | 4 | `nome_ubs` | `STRING` | Sim | NOME_UBS (descritivo; não é chave) |
# MAGIC | 5 | `sts_ubs` | `STRING` | Sim | STS_UBS como na origem |
# MAGIC | 6 | `crs_ubs` | `STRING` | Sim | CRS_UBS como na origem |
# MAGIC | 7 | `area_km2_texto` | `STRING` | Sim | AREAKM2 como na origem |
# MAGIC | 8 | `area_km2` | `DOUBLE` | Sim | AREAKM2 convertido (área oficial informada) |
# MAGIC | 9 | `status_conversao_area` | `STRING` | Sim | Resultado da conversão de AREAKM2 |
# MAGIC | 10 | `area_calculada_km2` | `DOUBLE` | Sim | Área calculada da geometria em EPSG:31983 (apenas validação) |
# MAGIC | 11 | `diferenca_area_km2` | `DOUBLE` | Sim | area_calculada_km2 - area_km2 |
# MAGIC | 12 | `status_area` | `STRING` | Sim | OK, DIVERGENTE, SEM_ATRIBUTO ou SEM_GEOMETRIA |
# MAGIC | 13 | `geometria_wkt` | `STRING` | Sim | MULTIPOLYGON em WKT (lon lat), texto original das coordenadas, sem Z |
# MAGIC | 14 | `srid` | `INT` | Sim | SRID da geometria (4674 = SIRGAS 2000 geográfico) |
# MAGIC | 15 | `crs_geometria` | `STRING` | Sim | Descrição do sistema de referência da geometria |
# MAGIC | 16 | `tipo_geometria_origem` | `STRING` | Sim | Elemento de geometria no KML de origem |
# MAGIC | 17 | `qtd_poligonos` | `INT` | Sim | Quantidade de polígonos |
# MAGIC | 18 | `qtd_aneis_internos` | `INT` | Sim | Quantidade de anéis internos (furos) |
# MAGIC | 19 | `qtd_vertices` | `INT` | Sim | Quantidade total de vértices |
# MAGIC | 20 | `problemas_geometria` | `ARRAY<STRING>` | Sim | Problemas estruturais encontrados (vazio = nenhum) |
# MAGIC | 21 | `geometria_valida` | `BOOLEAN` | Sim | Resultado de st_isvalid |
# MAGIC | 22 | `geometria_fora_bbox` | `BOOLEAN` | Sim | Vértice fora do envelope aproximado do MSP |
# MAGIC | 23 | `altitude_nao_zero` | `BOOLEAN` | Sim | Alguma coordenada com Z diferente de 0 |
# MAGIC | 24 | `cnes_no_cadastro` | `BOOLEAN` | Sim | CNES existe em ssp_silver_ubs_cadastro |
# MAGIC | 25 | `status_ponto_poligono` | `STRING` | Sim | DENTRO, FORA, SEM_COORDENADA ou GEOMETRIA_INVALIDA (st_covers do ponto do cadastro na área, por CNES) |
# MAGIC | 26 | `chaves_ausentes` | `ARRAY<STRING>` | Sim | Chaves esperadas ausentes na tabela HTML |
# MAGIC | 27 | `chaves_duplicadas` | `ARRAY<STRING>` | Sim | Chaves repetidas na tabela HTML |
# MAGIC | 28 | `atributos_extras_json` | `STRING` | Sim | Chaves não mapeadas (JSON) |
# MAGIC | 29 | `placemark_indice` | `INT` | Sim | Posição do placemark no doc.kml |
# MAGIC | 30 | `nome_placemark` | `STRING` | Sim | Nome do placemark (descritivo; não é chave) |
# MAGIC | 31 | `descricao_html_original` | `STRING` | Sim | Descrição HTML original (rastreabilidade) |
# MAGIC | 32 | `fonte_arquivo` | `STRING` | Sim | Caminho do KMZ na Bronze |
# MAGIC | 33 | `arquivo_interno` | `STRING` | Sim | Arquivo KML lido dentro do KMZ |
# MAGIC | 34 | `fonte_sha256` | `STRING` | Não | SHA-256 do arquivo Bronze processado |
# MAGIC | 35 | `id_ingestao_bronze` | `STRING` | Sim | id_ingestao do manifesto Bronze |
# MAGIC | 36 | `id_execucao` | `STRING` | Sim | Identificador desta execução do notebook 03 |
# MAGIC | 37 | `data_processamento` | `TIMESTAMP` | Sim | Data/hora do processamento (UTC) |

# COMMAND ----------

# DBTITLE 1,2.3 ssp_silver_censo
# MAGIC %md
# MAGIC ### 2.3 `workspace.default.ssp_silver_censo`
# MAGIC
# MAGIC * **Finalidade:** dados estimados do Censo 2022 agregados por área de abrangência de UBS, com a descrição de cada variável obtida da aba `dicionario_temas`.
# MAGIC * **Granularidade:** formato longo, uma linha por `cnes` × `aba_origem` × `variavel_origem`, com um valor por linha.
# MAGIC * **Registros:** 652.230 (485 CNES, 10 abas temáticas, 1.347 combinações distintas de aba × variável).
# MAGIC * **Comentário da tabela:** "Projeto Saúde Perto SP - Silver: Censo 2022 estimado por área de abrangência de UBS (formato longo). Chave: cnes + aba_origem + variavel_origem."
# MAGIC
# MAGIC | # | Coluna | Tipo | Nulo | Descrição (comentário do catálogo) |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1 | `cnes` | `STRING` | Não | CNES normalizado (texto, 7 dígitos) - chave de integração |
# MAGIC | 2 | `cnes_original` | `STRING` | Sim | CNES_UBS como na aba de origem |
# MAGIC | 3 | `aba_origem` | `STRING` | Não | Aba temática do Excel |
# MAGIC | 4 | `variavel_origem` | `STRING` | Não | Nome da coluna na aba (preservado) |
# MAGIC | 5 | `variavel_dicionario` | `STRING` | Sim | Código correspondente em dicionario_temas |
# MAGIC | 6 | `grupo_tema` | `STRING` | Sim | Grupo do tema no dicionário (Pessoas, Domicílio, Básico, Rendimento) |
# MAGIC | 7 | `tema` | `STRING` | Sim | Tema no dicionário |
# MAGIC | 8 | `descricao_variavel` | `STRING` | Sim | Descrição da variável no dicionário |
# MAGIC | 9 | `status_dicionario` | `STRING` | Sim | OK ou SEM_DESCRICAO |
# MAGIC | 10 | `valor` | `DOUBLE` | Sim | Valor estimado para a área, sem arredondamento (pode ser fracionário) |
# MAGIC | 11 | `status_valor` | `STRING` | Sim | OK, CONVERTIDO_TEXTO, VAZIO ou NAO_NUMERICO |
# MAGIC | 12 | `valor_texto_original` | `STRING` | Sim | Conteúdo original quando a célula não era numérica |
# MAGIC | 13 | `linha_origem` | `INT` | Sim | Linha da aba no Excel |
# MAGIC | 14 | `fonte_arquivo` | `STRING` | Sim | Caminho do Excel na Bronze |
# MAGIC | 15 | `fonte_sha256` | `STRING` | Não | SHA-256 do arquivo Bronze processado |
# MAGIC | 16 | `id_ingestao_bronze` | `STRING` | Sim | id_ingestao do manifesto Bronze |
# MAGIC | 17 | `id_execucao` | `STRING` | Sim | Identificador desta execução do notebook 04 |
# MAGIC | 18 | `data_processamento` | `TIMESTAMP` | Sim | Data/hora do processamento (UTC) |
# MAGIC
# MAGIC **Abas temáticas** (`aba_origem`): `basico`, `alfabetizacao`, `demografia`, `racacor`, `rendamediaresp`, `parentesco`, `domicilios1`, `domicilios2`, `domicilios3` e `FavelasComunidadesUrbanas`.
# MAGIC
# MAGIC **Glossário de siglas** (aba `siglas_basico`; documentado no notebook 04 e não gravado em tabela): DPPO = Domicílio Particular Permanente Ocupado; DPIO = Domicílio Particular Improvisado Ocupado; DPPV = Domicílio Particular Permanente Vago; DPPUO = Domicílio Particular Permanente de Uso Ocasional; DCCM = Domicílio Coletivo Com Morador; DCSM = Domicílio Coletivo Sem Morador; DPO = Domicílio Particular Ocupado (DPPO + DPIO).

# COMMAND ----------

# DBTITLE 1,3. Chaves e relacionamentos
# MAGIC %md
# MAGIC ## 3. Chaves e relacionamentos
# MAGIC
# MAGIC ### 3.1 CNES como identificador de integração
# MAGIC * `cnes` (`STRING NOT NULL`, 7 dígitos) é a **única** chave de integração entre as tabelas.
# MAGIC * Nomes de UBS (`nome_placemark`, `nome_divulgacao`, `nome_ubs`), STS e CRS são atributos descritivos e **não** são usados em junções.
# MAGIC * A mesma regra de normalização é aplicada nos notebooks 02, 03 e 04 (seção 4.1).
# MAGIC
# MAGIC ### 3.2 Chaves das tabelas
# MAGIC | Tabela | Chave lógica | Verificação implementada |
# MAGIC | --- | --- | --- |
# MAGIC | `ssp_silver_ubs_cadastro` | `cnes` | Regra bloqueante `CNES_UNICO`; após gravar, CNES distintos = linhas |
# MAGIC | `ssp_silver_areas_abrangencia` | `cnes` | Regra bloqueante `CNES_UNICO`; após gravar, CNES distintos = linhas |
# MAGIC | `ssp_silver_censo` | `cnes` + `aba_origem` + `variavel_origem` | Bloqueantes `CNES_UNICO_POR_ABA` e `COLUNAS_DUPLICADAS`; após gravar, chaves distintas = linhas |
# MAGIC
# MAGIC As chaves são **lógicas**, garantidas pelas validações dos notebooks. Não há restrições `PRIMARY KEY` ou `FOREIGN KEY` declaradas no catálogo.
# MAGIC
# MAGIC ### 3.3 Relacionamentos e cardinalidade
# MAGIC ```
# MAGIC ssp_silver_ubs_cadastro (1) ── cnes ── (1) ssp_silver_areas_abrangencia
# MAGIC ssp_silver_ubs_cadastro (1) ── cnes ── (N) ssp_silver_censo
# MAGIC ssp_silver_areas_abrangencia (1) ── cnes ── (N) ssp_silver_censo
# MAGIC ```
# MAGIC
# MAGIC | Relacionamento | Cardinalidade | Situação atual (consulta de leitura) |
# MAGIC | --- | --- | --- |
# MAGIC | Cadastro ↔ Áreas | 1 : 1 | 485 CNES nas duas tabelas; 0 cadastros sem área; 0 áreas sem cadastro (`cnes_no_cadastro` verdadeiro em todas) |
# MAGIC | Cadastro → Censo | 1 : N | Todos os 485 CNES têm linhas no Censo: 1.347 linhas para os 414 CNES com FCU e 1.332 para os 71 sem FCU |
# MAGIC | Áreas → Censo | 1 : N | 0 CNES do Censo sem área (regra bloqueante `CNES_SEM_AREA` no notebook 04) |
# MAGIC
# MAGIC Na aba `FavelasComunidadesUrbanas`, o relacionamento é **1 : 0..N**: 71 áreas não têm linhas dessa aba (seção 4.3).

# COMMAND ----------

# DBTITLE 1,3.4 Diagrama entidade-relacionamento (modelo lógico)
# MAGIC %md
# MAGIC ### 3.4 Diagrama entidade-relacionamento (modelo lógico)
# MAGIC
# MAGIC O diagrama mostra, para cada tabela, a quantidade de colunas, a granularidade, a chave lógica e a quantidade de linhas; a lista completa de colunas está na seção 2. **Todas as chaves e relacionamentos são lógicos**: os três relacionamentos usam a coluna `cnes`, e nenhuma restrição `PRIMARY KEY` ou `FOREIGN KEY` foi criada no catálogo. Por isso, o diagrama não usa os marcadores `PK`/`FK`; a chave lógica de cada tabela aparece na linha "Chave lógica" do seu quadro.
# MAGIC
# MAGIC **Diagrama entidade-relacionamento (modelo lógico)**
# MAGIC ```
# MAGIC ┌────────────────────────────────────────┐                ┌────────────────────────────────────────────┐
# MAGIC │ ssp_silver_ubs_cadastro (47 colunas)   │ 1            1 │ ssp_silver_areas_abrangencia (37 colunas)  │
# MAGIC │ Granularidade: 1 linha por UBS         │───── cnes ─────│ Granularidade: 1 linha por área/UBS        │
# MAGIC │ Chave lógica: cnes                     │ (lógico, 1:1)  │ Chave lógica: cnes                         │
# MAGIC │ 485 linhas                             │                │ 485 linhas                                 │
# MAGIC └────────────────────────────────────────┘                └────────────────────────────────────────────┘
# MAGIC                     │ 1                                                       │ 1
# MAGIC                     │ cnes (lógico, 1:N)                                      │ cnes (lógico, 1:N)
# MAGIC                     │ N                                                       │ N
# MAGIC                     ▼                                                         │
# MAGIC ┌────────────────────────────────────────────────────┐                        │
# MAGIC │ ssp_silver_censo (18 colunas)                      │◀───────────────────────┘
# MAGIC │ Granularidade: 1 linha por cnes × aba × variável   │
# MAGIC │ Chave lógica: cnes + aba_origem + variavel_origem  │
# MAGIC │ 652.230 linhas (1.332 a 1.347 por CNES)            │
# MAGIC └────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Legenda**
# MAGIC
# MAGIC | Elemento | Significado |
# MAGIC | --- | --- |
# MAGIC | Chave lógica | Coluna(s) que identificam uma linha, com unicidade garantida pelas regras bloqueantes dos notebooks 02, 03 e 04. Não é uma restrição `PRIMARY KEY` |
# MAGIC | Linha entre tabelas | Relacionamento lógico por `cnes` (igualdade de texto de 7 dígitos). Não é uma restrição `FOREIGN KEY` |
# MAGIC | Cardinalidade 1 : 1 | Cada UBS do cadastro tem exatamente uma área, e cada área tem exatamente uma UBS |
# MAGIC | Cardinalidade 1 : N | Cada CNES tem uma ou mais linhas no Censo (1.332 a 1.347). Na aba `FavelasComunidadesUrbanas`, o relacionamento é 1 : 0..N |
# MAGIC
# MAGIC **Granularidade**
# MAGIC
# MAGIC | Tabela | Colunas | Granularidade (uma linha por) | Chave lógica | Linhas |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | `ssp_silver_ubs_cadastro` | 47 | UBS | `cnes` | 485 |
# MAGIC | `ssp_silver_areas_abrangencia` | 37 | Área de abrangência (uma por UBS) | `cnes` | 485 |
# MAGIC | `ssp_silver_censo` | 18 | CNES × aba temática × variável | `cnes` + `aba_origem` + `variavel_origem` | 652.230 |

# COMMAND ----------

# DBTITLE 1,4. Regras de negócio e qualidade
# MAGIC %md
# MAGIC ## 4. Regras de negócio e qualidade
# MAGIC
# MAGIC Os três notebooks Silver classificam as regras em três níveis:
# MAGIC * **BLOQUEANTE:** se falhar, interrompe antes da gravação e nada é gravado.
# MAGIC * **ALERTA:** registrado e exibido, sem impedir a gravação.
# MAGIC * **INFO:** caracterização dos dados.
# MAGIC
# MAGIC ### 4.1 CNES: normalização e unicidade (02, 03 e 04)
# MAGIC * O CNES é tratado como texto. São removidos apenas os espaços e o sufixo `.0`. No notebook 04, um CNES numérico inteiro do Excel também é aceito.
# MAGIC * Só são aceitos valores com 1 a 7 dígitos. Valores mais curtos são completados com zeros à esquerda até 7 dígitos, com `status_cnes = COMPLETADO_ZEROS` (alerta).
# MAGIC * CNES ausente ou inválido é bloqueante (`CNES_VALIDO`), assim como CNES repetido (`CNES_UNICO`; no 04, `CNES_UNICO_POR_ABA`).
# MAGIC * Após a gravação, é conferido que todos os CNES seguem o padrão `^[0-9]{7}$`.
# MAGIC
# MAGIC ### 4.2 Cadastro: coordenadas (notebook 02)
# MAGIC * **Geometria:** o `Point` do KML é mantido como está (lon/lat, `EPSG:4326`, padrão KML 2.2). Cada placemark deve ter exatamente 1 `Point` e nenhum polígono (bloqueante `GEOMETRIA_PONTO_UNICO`), e as coordenadas devem ser numéricas (bloqueante `GEOMETRIA_VALIDA`).
# MAGIC * **Atributos:** `LONSIRGAS`/`LATSIRGAS` (`EPSG:4674`) são convertidos de vírgula decimal para número.
# MAGIC * **Comparação:** distância haversine entre geometria e atributos, sem correção:
# MAGIC   * até 1 m: `IDENTICA`;
# MAGIC   * de 1 a 50 m: `PROXIMA`;
# MAGIC   * acima de 50 m: `DIVERGENTE` (ou `DIVERGENTE_POSSIVEL_INVERSAO`, se a inversão lon/lat ficar a até 50 m).
# MAGIC * **Plausibilidade:** envelope aproximado do MSP (lon −46,83 a −46,36; lat −24,01 a −23,35), alerta `GEOMETRIA_FORA_BBOX`.
# MAGIC * **Outros alertas:** `ALTITUDE_NAO_ZERO`, `CEP_FORMATO` (padrão 00000-000), `COORD_ATRIBUTO_NAO_CONVERTIDA`, `CHAVES_AUSENTES`, `CHAVES_EXTRAS`, `CHAVES_DUPLICADAS` e `NOME_PLACEMARK_DIFERE_DIVULGACAO`.
# MAGIC * Texto vazio vira `NULL`, e a contagem de vazios por coluna é informativa.
# MAGIC
# MAGIC ### 4.3 Áreas de abrangência: geometria (notebook 03)
# MAGIC * **Leitura:** o KMZ é aberto como ZIP somente para leitura. O `doc.kml` usa o prefixo `xsi` sem declará-lo; a declaração é incluída **apenas em memória**.
# MAGIC * **MultiGeometry e furos:** cada `MultiGeometry` vira um `MULTIPOLYGON` em WKT. Cada polígono mantém o anel externo e todos os anéis internos (furos). As coordenadas são copiadas do texto original, sem arredondamento, e a coordenada Z (sempre 0) é descartada e verificada (`altitude_nao_zero`).
# MAGIC * **Sistema de referência:** SIRGAS 2000 geográfico (`srid = 4674`), sem reprojeção. A projeção `EPSG:31983` (SIRGAS 2000 / UTM 23S) é usada apenas para calcular `area_calculada_km2` e compará-la com `AREAKM2`, com tolerância de 0,001 km² (`status_area`).
# MAGIC * **Funções espaciais nativas:** `st_geomfromwkt`, `st_isvalid`, `st_transform`, `st_area`, `st_point`, `st_covers`.
# MAGIC * **Bloqueantes:** `QTD_REGISTROS` (485), `CNES_VALIDO`, `CNES_UNICO`, `GEOMETRIA_ESTRUTURA` (anéis fechados com 4 ou mais pontos) e `GEOMETRIA_VALIDA` (`st_isvalid`).
# MAGIC * **Ponto × polígono:** o ponto vem de `ssp_silver_ubs_cadastro` (`geom_longitude`, `geom_latitude`), relacionado **somente pelo CNES**. `st_covers(área, ponto)` considera dentro também o ponto sobre a borda. O resultado fica em `status_ponto_poligono` (`DENTRO`, `FORA`, `SEM_COORDENADA`, `GEOMETRIA_INVALIDA`); `FORA` gera o alerta `PONTO_FORA_DA_AREA`. O ponto (WGS 84) é interpretado em SIRGAS 2000, diferença considerada desprezível nesta escala no notebook 03. Nenhuma geometria ou coordenada é corrigida.
# MAGIC * **Outros alertas:** `CNES_SEM_CADASTRO`, `AREA_NAO_CONVERTIDA`, `AREA_DIVERGENTE`, `GEOMETRIA_FORA_BBOX`, `ALTITUDE_NAO_ZERO`, `PONTO_SEM_COORDENADA`, `TIPO_GEOMETRIA_INESPERADO` e alertas de chaves HTML.
# MAGIC * **Informativas:** `MULTIPOLIGONO` e `COM_FUROS`.
# MAGIC
# MAGIC ### 4.4 Censo (notebook 04)
# MAGIC * **Abas e linhas:** as 10 abas temáticas têm as 4 colunas de identificação `CRS_UBS`, `STS_UBS`, `NOME_UBS`, `CNES_UBS` (bloqueante). A quantidade de áreas por aba é conferida (485; FCU 414, bloqueante).
# MAGIC * **`Total Geral`:** a linha de totalização de cada aba é removida (10 linhas) e usada só para conferir as somas.
# MAGIC * **Valores fracionários:** os valores são estimativas distribuídas por área e por isso podem ser fracionários. São gravados como `DOUBLE`, **sem arredondamento**. Texto numérico seria convertido (`CONVERTIDO_TEXTO`); célula vazia vira `NULL`, nunca zero.
# MAGIC * **Colunas não aditivas:** em 12 colunas o `Total Geral` não é a soma das áreas:
# MAGIC   * média e desvio padrão da renda (`MEAN_V06004AAUBS2`, `STD_V06004AAUBS`);
# MAGIC   * na FCU, `V001AA`, `V002AA`, `V003AA`, `V004AA`, `V007AA` e os percentuais `P_V00nAA_FCU` correspondentes.
# MAGIC
# MAGIC   Uma divergência fora dessa lista gera o alerta `TOTAL_GERAL_DIVERGENCIA_INESPERADA`.
# MAGIC * **Dicionário:** `variavel_dicionario` é obtida por regras de sufixo explícitas (`AAUBS`, `AA`, `_FCU`). Variáveis sem correspondência ficam com `status_dicionario = SEM_DESCRICAO` e descrição `NULL`, sem descrição inventada (alerta `VARIAVEL_SEM_DESCRICAO`).
# MAGIC * **Favelas e Comunidades Urbanas (FCU):** só as 414 áreas presentes na aba recebem linhas. As 71 restantes não recebem linha nem zero (informativo `AREAS_SEM_FCU`).
# MAGIC * **Integração:** `CNES_SEM_AREA` e `ABA_INCOMPLETA` são bloqueantes; `CNES_SEM_CADASTRO` é alerta.
# MAGIC * **Glossário:** a aba `siglas_basico` é relida e comparada com a documentação do notebook (alerta `SIGLAS_DIFEREM_DA_DOCUMENTACAO`).
# MAGIC
# MAGIC ### 4.5 Idempotência (02, 03 e 04)
# MAGIC 1. **Origem:** a fonte deve ter uma única versão no manifesto, e o SHA-256 do arquivo no Volume deve ser igual ao do manifesto.
# MAGIC 2. **Criação:** a tabela é criada com `CREATE TABLE IF NOT EXISTS`; se já existe, nada muda. O esquema existente é conferido antes de gravar.
# MAGIC 3. **Decisão de gravação:**
# MAGIC    * tabela vazia: grava em modo `append` (`GRAVADO`);
# MAGIC    * mesma versão da fonte e mesma quantidade de linhas: não grava (`JA_PROCESSADO`);
# MAGIC    * qualquer outra situação: interrompe com erro e nada é gravado.
# MAGIC 4. **Operações usadas:** nenhum notebook usa `overwrite`, `DELETE`, `DROP` ou `MERGE`.
# MAGIC 5. **Validação pós-gravação:** quantidade de linhas, unicidade da chave, padrão do CNES e versão única da fonte; no 04, também a soma por aba e a integridade com as outras tabelas.

# COMMAND ----------

# DBTITLE 1,5. Linhagem dos dados
# MAGIC %md
# MAGIC ## 5. Linhagem dos dados
# MAGIC
# MAGIC ```
# MAGIC Arquivos originais (/Workspace/Users/rm572201@fiap.com.br/Arquivos/)
# MAGIC │
# MAGIC │  00_setup_ambiente / 01_bronze_ingestao: cópia sem alteração + SHA-256
# MAGIC ▼
# MAGIC BRONZE: Volume workspace.default.ssp_raw_files + tabela ssp_bronze_manifest
# MAGIC │
# MAGIC ├─ consulta_ubs_kml/Consulta UBS.kml
# MAGIC │     └─ 02_silver_ubs_cadastro ────────────────────▶ SILVER ssp_silver_ubs_cadastro
# MAGIC │                                                                   │ (ponto por cnes)
# MAGIC ├─ aaubs_2026_kmz/Areas_de_Abrangencia_de_UBS_no_MSP_2026.kmz       ▼
# MAGIC │     └─ 03_silver_areas_abrangencia (+ leitura do cadastro) ─▶ SILVER ssp_silver_areas_abrangencia
# MAGIC │                                                                   │ (cnes)
# MAGIC └─ censo2022_aaubs_xlsx/Dados_Estimados_..._rev29052026.xlsx        ▼
# MAGIC       └─ 04_silver_censo (+ leitura de áreas e cadastro) ────▶ SILVER ssp_silver_censo
# MAGIC ```
# MAGIC
# MAGIC | Tabela Silver | Arquivo Bronze | Elemento lido | Dependências de leitura | `id_execucao` | Gravação (UTC) |
# MAGIC | --- | --- | --- | --- | --- | --- |
# MAGIC | `ssp_silver_ubs_cadastro` | `Consulta UBS.kml` | Pasta `Estabelecimento de Saúde Referencia da AA-UBS` (485 `Point`) | Manifesto | `2c8cb534-fafa-40e0-bd02-9ca41b5c89d1` | 16/11/2026 22:56 |
# MAGIC | `ssp_silver_areas_abrangencia` | `Areas_de_Abrangencia_de_UBS_no_MSP_2026.kmz` | `doc.kml` (485 `MultiGeometry`) | Manifesto, cadastro (ponto × polígono) | `bbe1f328-2d1b-4860-8a54-c3199cd2da2d` | 17/11/2026 17:21 |
# MAGIC | `ssp_silver_censo` | `Dados_Estimados_..._rev29052026.xlsx` | 10 abas temáticas + `dicionario_temas` (+ `siglas_basico` só para conferência) | Manifesto, áreas, cadastro | `bf7ceb0f-9e36-49a2-a79f-9f5f376eae21` | 17/11/2026 18:18 |
# MAGIC
# MAGIC As três tabelas Silver referenciam o mesmo `id_ingestao_bronze` (`e50fbead-2f52-4fda-8c0c-123d4babe96b`). O SHA-256 de cada fonte está em `fonte_sha256`, e por ele é possível voltar ao arquivo exato da Bronze.
# MAGIC
# MAGIC **Histórico Delta** (`DESCRIBE HISTORY`): cada tabela tem duas versões, 0 (`CREATE TABLE`) e 1 (`WRITE`, modo `Append`), com 485, 485 e 652.230 linhas gravadas. As reexecuções não geraram novas versões.

# COMMAND ----------

# DBTITLE 1,6. Resultados e limitações conhecidas
# MAGIC %md
# MAGIC ## 6. Resultados e limitações conhecidas
# MAGIC
# MAGIC ### 6.1 Resultados obtidos
# MAGIC Verificados por consultas de leitura nas tabelas gravadas.
# MAGIC
# MAGIC | Tabela | Resultado |
# MAGIC | --- | --- |
# MAGIC | `ssp_silver_ubs_cadastro` | 485 linhas e 485 CNES (todos `status_cnes = OK`). Coordenadas: 482 `IDENTICA` (até 0,845 m), 1 `PROXIMA`, 2 `DIVERGENTE`. 0 pontos fora do envelope, 0 CEPs fora do padrão, 0 atributos de coordenada não convertidos, 0 chaves ausentes, 0 altitudes diferentes de 0 |
# MAGIC | `ssp_silver_areas_abrangencia` | 485 linhas e 485 CNES. Todas `MultiGeometry` na origem e gravadas como `MULTIPOLYGON`, `srid` 4674. 485 geometrias válidas; 485 com `status_area = OK` (diferença máxima de 0,000078 km²). 487 polígonos; 2 áreas com 2 polígonos e 5 com 1 furo cada. 0 vértices fora do envelope; 0 CNES sem cadastro. Ponto × polígono: 479 `DENTRO`, 6 `FORA` |
# MAGIC | `ssp_silver_censo` | 652.230 linhas, 485 CNES, 10 abas, 1.347 variáveis; chave única; 0 valores nulos; 408.384 valores fracionários; 10 linhas `Total Geral` removidas; 12 colunas não aditivas iguais à lista definida; 5 variáveis `SEM_DESCRICAO` (2.212 linhas); 71 áreas sem FCU |
# MAGIC
# MAGIC Nas três tabelas, as regras bloqueantes foram aprovadas e a reexecução resultou em `JA_PROCESSADO`.
# MAGIC
# MAGIC ### 6.2 Pontos de atenção (alertas mantidos, sem correção)
# MAGIC Estes registros foram gravados normalmente. São alertas de qualidade, **não erros**.
# MAGIC
# MAGIC **Coordenadas do cadastro diferentes dos atributos:**
# MAGIC * `2788845`: `PROXIMA`, a 4,94 m;
# MAGIC * `2752352`: `DIVERGENTE`, a 71,07 m;
# MAGIC * `2788616`: `DIVERGENTE`, a 71,32 m.
# MAGIC
# MAGIC **Ponto da UBS fora da própria área (`status_ponto_poligono = FORA`):**
# MAGIC
# MAGIC | CNES | UBS (`nome_ubs`) | Distância até a própria área |
# MAGIC | --- | --- | --- |
# MAGIC | 3651762 | UBS MORRO DOCE | 11,9 m |
# MAGIC | 2068044 | UBS HORTO FLORESTAL | 17,2 m |
# MAGIC | 2815273 | UBS ZUMBI DOS PALMARES | 25,0 m |
# MAGIC | 2787342 | AMA/UBS JD DAS LARANJEIRAS | 215,2 m |
# MAGIC | 2751828 | UBS VL JOANIZA JOAO YUNES | 297,6 m |
# MAGIC | 2787814 | UBS JD SAO NORBERTO | 2.320,9 m |
# MAGIC
# MAGIC Nos 6 casos, a coordenada do cadastro é `IDENTICA` aos atributos. O nome no cadastro de `2787814` indica funcionamento provisório na UBS Jardim Iporã. As distâncias vêm da análise de leitura feita após o notebook 03 e não estão gravadas em coluna.
# MAGIC
# MAGIC **Censo:**
# MAGIC * 5 variáveis sem descrição no dicionário: `racacor.POP_NEGRAAAUBS`, `racacor.MNEGRAS_RESPDOMAAUBS`, `FavelasComunidadesUrbanas.V004AA_FCU`, `FavelasComunidadesUrbanas.V004AA`, `FavelasComunidadesUrbanas.P_V004AA_FCU`.
# MAGIC * 4 entradas do dicionário sem coluna nas abas: `DENSDOM`, `DENSDOMFCU`, `V0005`, `V0006`.
# MAGIC
# MAGIC ### 6.3 Limitações conhecidas
# MAGIC * **Somas no Censo:** valores de média, desvio padrão e percentual (colunas não aditivas) não devem ser somados entre áreas.
# MAGIC * **Ausência de FCU:** a falta de linhas de FCU em 71 áreas é ausência de registro na fonte. Em junções, aparece como `NULL`, não como zero.
# MAGIC * **Sistemas de referência:** o ponto do cadastro está em `EPSG:4326` (padrão KML) e o polígono em `EPSG:4674`. A comparação ponto × polígono interpreta o ponto em SIRGAS 2000.
# MAGIC * **Geometria em texto:** as geometrias são armazenadas como texto WKT (`STRING`). Análises espaciais exigem conversão com `st_geomfromwkt`.
# MAGIC * **Integridade:** as chaves são lógicas e não há restrições `PRIMARY KEY`/`FOREIGN KEY` declaradas. A integridade é garantida pelas validações dos notebooks.
# MAGIC * **Nova versão de fonte:** um reprocessamento com outra versão de arquivo interrompe a execução e exige decisão explícita; não há histórico de múltiplas versões na Silver.

# COMMAND ----------

# DBTITLE 1,7. Exemplos de consultas
# MAGIC %md
# MAGIC ## 7. Exemplos de consultas
# MAGIC
# MAGIC Consultas SQL de leitura. O CNES `2026961` é usado como exemplo.
# MAGIC
# MAGIC **7.1 Consultar uma UBS pelo CNES**
# MAGIC ```sql
# MAGIC SELECT cnes, nome_divulgacao, logradouro, numero_logradouro, bairro, cep, telefone_1,
# MAGIC        supervisao_tecnica_saude, coordenadoria_regional_saude,
# MAGIC        geom_longitude, geom_latitude, status_coordenada
# MAGIC FROM workspace.default.ssp_silver_ubs_cadastro
# MAGIC WHERE cnes = '2026961';
# MAGIC ```
# MAGIC
# MAGIC **7.2 Consultar a área de abrangência**
# MAGIC ```sql
# MAGIC SELECT cnes, nome_ubs, area_km2, area_calculada_km2, qtd_poligonos, qtd_aneis_internos,
# MAGIC        status_ponto_poligono, srid,
# MAGIC        st_geomfromwkt(geometria_wkt, srid) AS geometria
# MAGIC FROM workspace.default.ssp_silver_areas_abrangencia
# MAGIC WHERE cnes = '2026961';
# MAGIC ```
# MAGIC
# MAGIC **7.3 Consultar os indicadores do Censo de uma área**
# MAGIC ```sql
# MAGIC SELECT aba_origem, variavel_origem, variavel_dicionario, tema, descricao_variavel, valor, status_dicionario
# MAGIC FROM workspace.default.ssp_silver_censo
# MAGIC WHERE cnes = '2026961'
# MAGIC   AND aba_origem = 'basico'
# MAGIC ORDER BY variavel_origem;
# MAGIC ```
# MAGIC
# MAGIC **7.4 Relacionar as três tabelas pelo CNES**
# MAGIC Total de pessoas (`basico.V001AA`) e população em favelas e comunidades urbanas (`FavelasComunidadesUrbanas.V001AA_FCU`) por UBS. O `LEFT JOIN` mantém as 71 áreas sem FCU com `NULL`, sem zero.
# MAGIC ```sql
# MAGIC SELECT u.cnes,
# MAGIC        u.nome_divulgacao,
# MAGIC        u.coordenadoria_regional_saude,
# MAGIC        a.area_km2,
# MAGIC        a.status_ponto_poligono,
# MAGIC        pop.valor AS total_pessoas,
# MAGIC        fcu.valor AS pessoas_fcu
# MAGIC FROM workspace.default.ssp_silver_ubs_cadastro u
# MAGIC JOIN workspace.default.ssp_silver_areas_abrangencia a
# MAGIC   ON a.cnes = u.cnes
# MAGIC LEFT JOIN workspace.default.ssp_silver_censo pop
# MAGIC   ON pop.cnes = u.cnes AND pop.aba_origem = 'basico' AND pop.variavel_origem = 'V001AA'
# MAGIC LEFT JOIN workspace.default.ssp_silver_censo fcu
# MAGIC   ON fcu.cnes = u.cnes AND fcu.aba_origem = 'FavelasComunidadesUrbanas' AND fcu.variavel_origem = 'V001AA_FCU'
# MAGIC ORDER BY u.cnes;
# MAGIC ```