# Databricks notebook source
# DBTITLE 1,Apresentação do notebook
# MAGIC %md
# MAGIC # 04 — Silver: Censo 2022 por área de abrangência | Projeto Saúde Perto SP
# MAGIC
# MAGIC **Objetivo:** estruturar os dados estimados do Censo 2022 por área de abrangência de UBS (AAUBS 2026), a partir da cópia Bronze do arquivo Excel. Sem camada Gold, indicadores derivados ou dashboard.
# MAGIC
# MAGIC **Fonte:** `Dados_Estimados_Agregados_por_setores_UniversoCenso2022_MSP_SC_AAUBS2026_rev29052026.xlsx`
# MAGIC * **Abas temáticas (10):** `basico`, `alfabetizacao`, `demografia`, `racacor`, `rendamediaresp`, `parentesco`, `domicilios1`, `domicilios2`, `domicilios3` (485 áreas cada) e `FavelasComunidadesUrbanas` (414 áreas).
# MAGIC * **`dicionario_temas`:** descrição de cada variável (grupo, tema, variável, descrição).
# MAGIC * **`siglas_basico`:** glossário das siglas de domicílio usadas nas descrições. Fica documentado neste notebook (célula *Glossário de siglas* e conferência na célula 3) e **não é gravado** em tabela.
# MAGIC * **Não carregadas:** `metodo_elaboracao` (texto metodológico) e `agreg_espaciais_AAUBS_STS_CRS` (lista de áreas, já coberta por `ssp_silver_areas_abrangencia`).
# MAGIC
# MAGIC **Saída:** `workspace.default.ssp_silver_censo`, em formato longo: uma linha por **CNES × aba × variável**, com o valor e a descrição do dicionário. As validações são exibidas no notebook. Não há tabela de qualidade separada.
# MAGIC
# MAGIC **Regras de transformação**
# MAGIC 1. **CNES:** texto de 7 dígitos, mesma regra dos notebooks 02 e 03 (também aceita CNES numérico do Excel). É a única chave de integração; nome, STS e CRS das abas não são gravados porque já estão nas tabelas Silver de áreas e cadastro.
# MAGIC 2. **`Total Geral`:** a linha de totalização de cada aba é removida e usada apenas para conferir as somas.
# MAGIC 3. **Variáveis:** o nome da coluna é mantido como na aba (`variavel_origem`). O código do dicionário (`variavel_dicionario`) é obtido por regras explícitas de sufixo (`AAUBS`, `AA`, `_FCU`); variáveis sem correspondência ficam com `status_dicionario = SEM_DESCRICAO`, sem descrição inventada.
# MAGIC 4. **Valores:** gravados como `DOUBLE`, **sem arredondamento**, preservando as frações da distribuição espacial. Texto numérico é convertido e registrado; célula vazia vira `NULL` (nunca zero).
# MAGIC 5. **Favelas e Comunidades Urbanas:** somente as 414 áreas presentes na aba recebem linhas. As demais **não recebem linha nem zero**; a ausência é preservada.
# MAGIC 6. **Medidas não aditivas** (médias, desvios, variâncias e percentuais) são mantidas como vêm; a descrição do dicionário indica sua natureza.
# MAGIC
# MAGIC **Dependência:** `openpyxl` (leitura de `.xlsx`), mesma versão do notebook 00. É instalada no escopo deste notebook.
# MAGIC
# MAGIC **Idempotência:** tabela vazia → grava; mesma versão da fonte (SHA-256) já gravada → não regrava; versão diferente → interrompe. Nada é sobrescrito.

# COMMAND ----------

# DBTITLE 1,Glossário de siglas (aba siglas_basico)
# MAGIC %md
# MAGIC ## Glossário de siglas — aba `siglas_basico`
# MAGIC
# MAGIC Conteúdo transcrito da planilha, sem alteração. Na origem, o cabeçalho das colunas é `CNESAAUBS` | `Descrição`; a primeira coluna contém as siglas. As siglas aparecem nas descrições do `dicionario_temas` (ex.: `V0002` = "Total de Domicílios (DPPO + DPPV + DPPUO + DPIO + DCCM + DCSM)").
# MAGIC
# MAGIC | Sigla | Descrição |
# MAGIC | --- | --- |
# MAGIC | DPPO | Domicílio Particular Permanente Ocupado |
# MAGIC | DPIO | Domicílio Particular Improvisado Ocupado |
# MAGIC | DPPV | Domicílio Particular Permanente Vago |
# MAGIC | DPPUO | Domicílio Particular Permanente de Uso Ocasional |
# MAGIC | DCCM | Domicílio Coletivo Com Morador |
# MAGIC | DCSM | Domicílio Coletivo Sem Morador |
# MAGIC | DPO | Domicílio Particular Ocupado (DPPO + DPIO) |
# MAGIC
# MAGIC Este glossário **não é gravado** em `ssp_silver_censo` nem em outra tabela. A célula 3 relê a aba e confere com `SIGLAS_DOCUMENTADAS` (célula 1); diferenças geram o alerta `SIGLAS_DIFEREM_DA_DOCUMENTACAO`.

# COMMAND ----------

# DBTITLE 1,0. Dependência: leitura de Excel
# MAGIC %pip install -q openpyxl==3.1.5

# COMMAND ----------

# DBTITLE 1,1. Parâmetros do projeto
# Parâmetros (mesma nomenclatura dos notebooks 00 a 03)
PREFIXO = "ssp"
CATALOGO = "workspace"
SCHEMA = "default"
TABELA_MANIFESTO = f"{CATALOGO}.{SCHEMA}.{PREFIXO}_bronze_manifest"
TABELA_CADASTRO = f"{CATALOGO}.{SCHEMA}.{PREFIXO}_silver_ubs_cadastro"        # somente leitura
TABELA_AREAS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}_silver_areas_abrangencia"       # somente leitura
TABELA_CENSO = f"{CATALOGO}.{SCHEMA}.{PREFIXO}_silver_censo"

FONTE = "censo2022_aaubs_xlsx"
ABA_DICIONARIO = "dicionario_temas"
ABA_SIGLAS = "siglas_basico"          # somente documentação (não é gravada)
QTD_AREAS = 485

# Abas temáticas e quantidade esperada de áreas (CNES) em cada uma
ABAS_TEMATICAS = {
    "basico": 485, "alfabetizacao": 485, "demografia": 485, "racacor": 485, "rendamediaresp": 485,
    "parentesco": 485, "domicilios1": 485, "domicilios2": 485, "domicilios3": 485,
    "FavelasComunidadesUrbanas": 414,
}
ABA_FCU = "FavelasComunidadesUrbanas"
COLUNAS_IDENTIFICACAO = ["CRS_UBS", "STS_UBS", "NOME_UBS", "CNES_UBS"]   # 4 primeiras colunas de cada aba
ROTULO_TOTAL = "total geral"

# Correspondência entre o nome da coluna na aba e o código no dicionário (primeira regra que casar vale).
# Ex.: V00644AAUBS -> V00644 | V001AA -> V0001 | V001AA_FCU -> v0001FCU | P_V001AA_FCU -> Pv0001FCU
REGRAS_DICIONARIO = {
    "basico": [(r"V(\d{3})AA", r"V0\1")],
    ABA_FCU: [(r"V(\d{3})AA_FCU", r"v0\1FCU"), (r"P_V(\d{3})AA_FCU", r"Pv0\1FCU"), (r"V(\d{3})AA", r"v0\1AAUBS")],
    "_padrao": [(r"(.+?)AAUBS2?", r"\1")],
}
TOLERANCIA_SOMA = 1e-6   # tolera apenas arredondamento de ponto flutuante na conferência com o Total Geral

# Colunas cuja linha Total Geral NÃO é a soma das áreas (identificadas na análise do arquivo):
# média e desvio padrão da renda; na FCU, os totais gerais (Total Geral = município inteiro) e os percentuais
COLUNAS_NAO_ADITIVAS = {
    "rendamediaresp.MEAN_V06004AAUBS2", "rendamediaresp.STD_V06004AAUBS",
    *[f"{ABA_FCU}.V00{n}AA" for n in (1, 2, 3, 4, 7)],
    *[f"{ABA_FCU}.P_V00{n}AA_FCU" for n in (1, 2, 3, 4, 7)],
}

# Glossário da aba siglas_basico, como consta na planilha (o mesmo da célula "Glossário de siglas").
# Usado apenas para conferir se a documentação continua igual à fonte.
SIGLAS_DOCUMENTADAS = {
    "DPPO": "Domicílio Particular Permanente Ocupado",
    "DPIO": "Domicílio Particular Improvisado Ocupado",
    "DPPV": "Domicílio Particular Permanente Vago",
    "DPPUO": "Domicílio Particular Permanente de Uso Ocasional",
    "DCCM": "Domicílio Coletivo Com Morador",
    "DCSM": "Domicílio Coletivo Sem Morador",
    "DPO": "Domicílio Particular Ocupado (DPPO + DPIO)",
}

print("Tabela de destino:", TABELA_CENSO)

# COMMAND ----------

# DBTITLE 1,2. Localização e verificação do Excel na Bronze
import re, math, json, hashlib, uuid, datetime, collections
import pandas as pd
import openpyxl

def sha256_arquivo(caminho, bloco=1024 * 1024):
    h = hashlib.sha256()
    with open(caminho, "rb") as f:
        for parte in iter(lambda: f.read(bloco), b""):
            h.update(parte)
    return h.hexdigest()

# A fonte deve ter uma única versão (SHA-256) registrada no manifesto Bronze
versoes = spark.sql(
    f"""SELECT DISTINCT sha256, caminho_bronze FROM {TABELA_MANIFESTO}
        WHERE fonte = :f AND status IN ('INGERIDO', 'JA_INGERIDO')""",
    args={"f": FONTE},
).collect()
assert len(versoes) == 1, f"Esperada 1 versão de {FONTE} no manifesto; encontradas {len(versoes)}"

caminho_xlsx = versoes[0]["caminho_bronze"]
sha_manifesto = versoes[0]["sha256"]
id_ingestao_bronze = spark.sql(
    f"""SELECT id_ingestao FROM {TABELA_MANIFESTO}
        WHERE fonte = :f AND sha256 = :h AND status = 'INGERIDO'
        ORDER BY data_ingestao LIMIT 1""",
    args={"f": FONTE, "h": sha_manifesto},
).first()["id_ingestao"]

# O arquivo no Volume deve ser exatamente o registrado na Bronze
sha_atual = sha256_arquivo(caminho_xlsx)
assert sha_atual == sha_manifesto, "Arquivo Bronze difere do SHA-256 do manifesto; processamento interrompido"

print("Arquivo:", caminho_xlsx)
print("SHA-256:", sha_atual, "| id_ingestao Bronze:", id_ingestao_bronze)

# COMMAND ----------

# DBTITLE 1,3. Abas do arquivo, dicionário de variáveis e glossário de siglas
# Somente leitura; data_only=True lê os valores já calculados das células
wb = openpyxl.load_workbook(caminho_xlsx, read_only=True, data_only=True)
print("Abas do arquivo:", wb.sheetnames)
faltando = [a for a in [ABA_DICIONARIO, *ABAS_TEMATICAS] if a not in wb.sheetnames]
assert not faltando, f"Abas esperadas ausentes: {faltando}"

def texto(valor):
    """Texto sem espaços nas pontas; None para vazio."""
    if valor is None:
        return None
    t = str(valor).strip()
    return t if t else None

def linhas_aba(nome):
    """Linhas não vazias da aba, com o número da linha no Excel (as abas têm linhas vazias de preenchimento)."""
    return [(n, linha) for n, linha in enumerate(wb[nome].iter_rows(values_only=True), start=1)
            if any(v is not None for v in linha)]

# Dicionário: colunas [grupo, Tema, Variável, Descrição]. O cabeçalho da 1ª coluna é 'CNESAAUBS' na origem,
# mas o conteúdo é o grupo do tema (Pessoas, Domicílio, Básico, Rendimento).
linhas_dic = linhas_aba(ABA_DICIONARIO)
cabecalho_dic = [texto(v) for v in linhas_dic[0][1][:4]]
print("Cabeçalho do dicionário:", cabecalho_dic)
assert cabecalho_dic[1:4] == ["Tema", "Variável", "Descrição"], "Estrutura do dicionário diferente da esperada"

dicionario, variaveis_dic_duplicadas = {}, []
for _, linha in linhas_dic[1:]:
    variavel = texto(linha[2])
    if variavel is None:
        continue
    if variavel in dicionario:
        variaveis_dic_duplicadas.append(variavel)
    dicionario[variavel] = {"grupo_tema": texto(linha[0]), "tema": texto(linha[1]), "descricao_variavel": texto(linha[3])}

print(f"Variáveis no dicionário: {len(dicionario)} | duplicadas: {variaveis_dic_duplicadas}")
print("Variáveis por tema:", collections.Counter(d["tema"] for d in dicionario.values()))

# Glossário de siglas (somente documentação: exibido e conferido, não gravado nem misturado aos valores)
siglas_fonte = {}
if ABA_SIGLAS in wb.sheetnames:
    linhas_siglas = linhas_aba(ABA_SIGLAS)
    print("Cabeçalho original de siglas_basico:", [texto(v) for v in linhas_siglas[0][1]])
    siglas_fonte = {texto(l[0]): texto(l[1]) for _, l in linhas_siglas[1:] if texto(l[0])}
    display(pd.DataFrame(list(siglas_fonte.items()), columns=["sigla", "descricao"]))
siglas_divergentes = sorted(set(siglas_fonte.items()) ^ set(SIGLAS_DOCUMENTADAS.items()))
print("Glossário documentado confere com a planilha:", not siglas_divergentes)

# COMMAND ----------

# DBTITLE 1,4. Leitura das abas temáticas em formato longo
def normalizar_cnes(valor):
    """CNES como texto de 7 dígitos (regra dos notebooks 02 e 03; aceita número do Excel). Retorna (cnes ou None, status)."""
    if valor is None:
        return None, "AUSENTE"
    if isinstance(valor, float):
        if not valor.is_integer():
            return None, "INVALIDO"
        valor = int(valor)
    v = re.sub(r"\.0+$", "", str(valor).strip())
    if not re.fullmatch(r"\d{1,7}", v):
        return None, "INVALIDO"
    return v.zfill(7), ("OK" if len(v) == 7 else "COMPLETADO_ZEROS")

def converter_valor(valor):
    """Converte a célula em float, sem arredondar. Retorna (valor ou None, status, texto original quando não era número)."""
    if valor is None:
        return None, "VAZIO", None
    if isinstance(valor, bool):
        return None, "NAO_NUMERICO", str(valor)
    if isinstance(valor, (int, float)):
        return (None, "VAZIO", None) if math.isnan(valor) else (float(valor), "OK", None)
    t = str(valor).strip()
    if not t:
        return None, "VAZIO", None
    try:   # texto numérico: vírgula decimal no padrão brasileiro
        return float(t.replace(".", "").replace(",", ".") if "," in t else t), "CONVERTIDO_TEXTO", t
    except ValueError:
        return None, "NAO_NUMERICO", t

def eh_total(linha):
    return any(texto(v) is not None and texto(v).casefold() == ROTULO_TOTAL for v in linha[:4])

valores = []                       # uma tupla por CNES x aba x variável
resumo_abas, cnes_nao_ok, totais_excel = [], [], {}
for aba, esperado in ABAS_TEMATICAS.items():
    linhas = linhas_aba(aba)
    cabecalho = [texto(c) for c in linhas[0][1]]
    variaveis = [(j, c) for j, c in enumerate(cabecalho) if j >= len(COLUNAS_IDENTIFICACAO) and c is not None]
    nomes = [c for _, c in variaveis]
    qtd_total = qtd_dados = celulas_sem_cabecalho = 0
    cnes_aba = []
    for n_linha, linha in linhas[1:]:
        linha = list(linha) + [None] * (len(cabecalho) - len(linha))
        if eh_total(linha):
            qtd_total += 1
            totais_excel[aba] = {c: linha[j] for j, c in variaveis}   # usado só para conferência
            continue
        qtd_dados += 1
        celulas_sem_cabecalho += sum(1 for j, c in enumerate(cabecalho) if c is None and linha[j] is not None)
        cnes, status_cnes = normalizar_cnes(linha[3])
        if status_cnes != "OK":
            cnes_nao_ok.append({"aba": aba, "linha_origem": n_linha, "cnes_original": texto(linha[3]), "status_cnes": status_cnes})
        cnes_aba.append(cnes)
        for j, coluna in variaveis:
            valor, status_valor, texto_original = converter_valor(linha[j])
            valores.append((cnes, texto(linha[3]), aba, coluna, valor, status_valor, texto_original, n_linha))
    resumo_abas.append({
        "aba": aba, "linhas_dados": qtd_dados, "linhas_esperadas": esperado,
        "cnes_distintos": len({c for c in cnes_aba if c}), "linhas_total_geral_removidas": qtd_total,
        "variaveis": len(nomes), "colunas_duplicadas": len(nomes) - len(set(nomes)),
        "cabecalho_identificacao_ok": cabecalho[:4] == COLUNAS_IDENTIFICACAO,
        "celulas_sem_cabecalho": celulas_sem_cabecalho, "cnes": cnes_aba,
    })
wb.close()   # leitura concluída (read_only mantém o arquivo aberto). Para reexecutar esta célula, reexecute antes a célula 3.

# Cópia em pandas apenas para conferências e exibição (a gravação usa a lista `valores`, que mantém None como NULL)
df_valores = pd.DataFrame(valores, columns=["cnes", "cnes_original", "aba_origem", "variavel_origem", "valor",
                                            "status_valor", "valor_texto_original", "linha_origem"])
display(pd.DataFrame(resumo_abas).drop(columns="cnes"))
print(f"Valores (CNES x aba x variável): {len(df_valores)}")
print("status_valor:", df_valores.status_valor.value_counts().to_dict())

# Conferência com a linha Total Geral (removida): soma da coluna x total informado
somas = df_valores.groupby(["aba_origem", "variavel_origem"]).valor.sum()
total_diverge = [f"{aba}.{var}" for aba, tot in totais_excel.items() for var, v in tot.items()
                 if isinstance(v, (int, float)) and abs(somas[(aba, var)] - v) > TOLERANCIA_SOMA * max(1.0, abs(v))]
print(f"Colunas cuja soma difere do Total Geral: {len(total_diverge)} (esperado em médias, desvios e percentuais)")
print(total_diverge)

# COMMAND ----------

# DBTITLE 1,5. Dicionário, integração por CNES e regras de validação
def codigo_dicionario(aba, coluna):
    """Código da variável no dicionário, pelas regras de REGRAS_DICIONARIO; sem regra aplicável, o próprio nome."""
    for padrao, troca in REGRAS_DICIONARIO.get(aba, REGRAS_DICIONARIO["_padrao"]):
        m = re.fullmatch(padrao, coluna)
        if m:
            return m.expand(troca)
    return coluna

# Uma linha por (aba, variável): código e descrição do dicionário
mapa_variaveis = []
for aba, var in df_valores[["aba_origem", "variavel_origem"]].drop_duplicates().itertuples(index=False):
    codigo = codigo_dicionario(aba, var)
    d = dicionario.get(codigo)
    mapa_variaveis.append((aba, var, codigo if d else None, d["grupo_tema"] if d else None,
                           d["tema"] if d else None, d["descricao_variavel"] if d else None,
                           "OK" if d else "SEM_DESCRICAO"))
df_mapa = pd.DataFrame(mapa_variaveis, columns=["aba_origem", "variavel_origem", "variavel_dicionario", "grupo_tema",
                                                "tema", "descricao_variavel", "status_dicionario"])
dicionario_sem_coluna = sorted(set(dicionario) - set(df_mapa.variavel_dicionario.dropna()))

# Integração: CNES das tabelas Silver já gravadas (somente leitura)
cnes_areas = {r["cnes"] for r in spark.table(TABELA_AREAS).select("cnes").collect()}
cnes_cadastro = {r["cnes"] for r in spark.table(TABELA_CADASTRO).select("cnes").collect()}
cnes_por_aba = {r["aba"]: r["cnes"] for r in resumo_abas}

id_execucao = str(uuid.uuid4())
data_execucao = datetime.datetime.now(datetime.timezone.utc)

regras = []
def registrar(regra, descricao, severidade, afetados):
    regras.append({"regra": regra, "descricao": descricao, "severidade": severidade,
                   "qtd_afetados": len(afetados), "aprovado": len(afetados) == 0,
                   "afetados": [str(a) for a in afetados]})

def celulas(status):
    return [f"{r.aba_origem}|{r.variavel_origem}|linha {r.linha_origem}"
            for r in df_valores[df_valores.status_valor == status].itertuples()]

# Bloqueantes: impedem a gravação
registrar("CABECALHO_IDENTIFICACAO", f"4 primeiras colunas devem ser {COLUNAS_IDENTIFICACAO}", "BLOQUEANTE",
          [r["aba"] for r in resumo_abas if not r["cabecalho_identificacao_ok"]])
registrar("QTD_LINHAS_POR_ABA", "Quantidade de áreas por aba igual à esperada (485; FCU 414)", "BLOQUEANTE",
          [f"{r['aba']}: {r['linhas_dados']} (esperado {r['linhas_esperadas']})" for r in resumo_abas
           if r["linhas_dados"] != r["linhas_esperadas"]])
registrar("CNES_VALIDO", "CNES presente e com 1 a 7 dígitos", "BLOQUEANTE",
          [f"{c['aba']}|linha {c['linha_origem']}|{c['cnes_original']}" for c in cnes_nao_ok
           if c["status_cnes"] in ("AUSENTE", "INVALIDO")])
registrar("CNES_UNICO_POR_ABA", "CNES não pode se repetir dentro da mesma aba", "BLOQUEANTE",
          [f"{aba}|{c}" for aba, lista in cnes_por_aba.items() for c, n in collections.Counter(lista).items() if c and n > 1])
registrar("COLUNAS_DUPLICADAS", "Nome de variável repetido na mesma aba", "BLOQUEANTE",
          [r["aba"] for r in resumo_abas if r["colunas_duplicadas"] > 0])
registrar("CELULA_SEM_CABECALHO", "Dado em coluna sem nome de variável (seria perdido)", "BLOQUEANTE",
          [r["aba"] for r in resumo_abas if r["celulas_sem_cabecalho"] > 0])
registrar("CNES_SEM_AREA", "CNES do Censo ausente em ssp_silver_areas_abrangencia", "BLOQUEANTE",
          sorted({c for lista in cnes_por_aba.values() for c in lista if c and c not in cnes_areas}))
registrar("ABA_INCOMPLETA", f"Abas com {QTD_AREAS} áreas devem conter todos os CNES de ssp_silver_areas_abrangencia", "BLOQUEANTE",
          [f"{aba}: faltam {len(cnes_areas - set(cnes_por_aba[aba]))}" for aba, esp in ABAS_TEMATICAS.items()
           if esp == QTD_AREAS and set(cnes_por_aba[aba]) != cnes_areas])

# Alertas: registrados, não impedem a gravação
registrar("CNES_COMPLETADO_ZEROS", "CNES com menos de 7 dígitos completado com zeros", "ALERTA",
          [f"{c['aba']}|linha {c['linha_origem']}|{c['cnes_original']}" for c in cnes_nao_ok if c["status_cnes"] == "COMPLETADO_ZEROS"])
registrar("CNES_SEM_CADASTRO", "CNES do Censo ausente em ssp_silver_ubs_cadastro", "ALERTA",
          sorted({c for lista in cnes_por_aba.values() for c in lista if c and c not in cnes_cadastro}))
registrar("VALOR_NAO_NUMERICO", "Célula com texto não numérico (valor NULL; texto preservado)", "ALERTA", celulas("NAO_NUMERICO"))
registrar("VALOR_CONVERTIDO_TEXTO", "Número armazenado como texto e convertido", "ALERTA", celulas("CONVERTIDO_TEXTO"))
registrar("VALOR_VAZIO", "Célula vazia (mantida como NULL, não zero)", "ALERTA", celulas("VAZIO"))
registrar("VALOR_NEGATIVO", "Valor negativo", "ALERTA",
          [f"{r.aba_origem}|{r.variavel_origem}|linha {r.linha_origem}" for r in df_valores[df_valores.valor < 0].itertuples()])
registrar("VARIAVEL_SEM_DESCRICAO", "Variável sem correspondência no dicionário", "ALERTA",
          [f"{r.aba_origem}|{r.variavel_origem}" for r in df_mapa[df_mapa.status_dicionario != "OK"].itertuples()])
registrar("DICIONARIO_DUPLICADO", "Variável repetida no dicionário (mantida a última descrição)", "ALERTA", variaveis_dic_duplicadas)
registrar("TOTAL_GERAL_DIVERGENCIA_INESPERADA", "Coluna aditiva cuja soma difere do Total Geral", "ALERTA",
          sorted(set(total_diverge) - COLUNAS_NAO_ADITIVAS))
registrar("NAO_ADITIVA_SEM_DIVERGENCIA", "Coluna listada como não aditiva cuja soma passou a conferir com o Total Geral", "ALERTA",
          sorted(COLUNAS_NAO_ADITIVAS - set(total_diverge)))
registrar("SIGLAS_DIFEREM_DA_DOCUMENTACAO", "Glossário siglas_basico diferente do documentado no notebook", "ALERTA",
          [f"{s}: {d}" for s, d in siglas_divergentes])

# Informativas
registrar("TOTAL_GERAL_REMOVIDO", "Linhas 'Total Geral' removidas por aba", "INFO",
          [f"{r['aba']}: {r['linhas_total_geral_removidas']}" for r in resumo_abas if r["linhas_total_geral_removidas"] > 0])
registrar("TOTAL_GERAL_DIFERE_DA_SOMA", f"Colunas cuja soma difere do Total Geral ({len(COLUNAS_NAO_ADITIVAS)} não aditivas esperadas)", "INFO", total_diverge)
registrar("DICIONARIO_SEM_COLUNA", "Variáveis do dicionário sem coluna nas abas temáticas", "INFO", dicionario_sem_coluna)
registrar("AREAS_SEM_FCU", "Áreas sem registro na aba FCU (ausência preservada, sem linhas com zero)", "INFO",
          sorted(cnes_areas - set(cnes_por_aba[ABA_FCU])))

df_regras = pd.DataFrame(regras)
display(df_regras[["regra", "severidade", "qtd_afetados", "aprovado", "descricao"]])

df_afetados = (df_regras[(df_regras.severidade != "INFO") & (~df_regras.aprovado)]
               [["regra", "severidade", "afetados"]].explode("afetados"))
if df_afetados.empty:
    print("Nenhum registro afetado por regras bloqueantes ou de alerta.")
else:
    display(df_afetados.rename(columns={"afetados": "registro"}))

bloqueios = df_regras[(df_regras.severidade == "BLOQUEANTE") & (~df_regras.aprovado)]
if not bloqueios.empty:
    raise RuntimeError(f"Regras bloqueantes reprovadas: {bloqueios.regra.tolist()}. Nada foi gravado.")
print("Regras bloqueantes: APROVADAS")

# COMMAND ----------

# DBTITLE 1,6. Criação não destrutiva da tabela Silver
# Definição única das colunas: gera o DDL e a ordem/tipos da gravação
COLUNAS = [
    ("cnes", "STRING", False, "CNES normalizado (texto, 7 dígitos) - chave de integração"),
    ("cnes_original", "STRING", True, "CNES_UBS como na aba de origem"),
    ("aba_origem", "STRING", False, "Aba temática do Excel"),
    ("variavel_origem", "STRING", False, "Nome da coluna na aba (preservado)"),
    ("variavel_dicionario", "STRING", True, "Código correspondente em dicionario_temas"),
    ("grupo_tema", "STRING", True, "Grupo do tema no dicionário (Pessoas, Domicílio, Básico, Rendimento)"),
    ("tema", "STRING", True, "Tema no dicionário"),
    ("descricao_variavel", "STRING", True, "Descrição da variável no dicionário"),
    ("status_dicionario", "STRING", True, "OK ou SEM_DESCRICAO"),
    ("valor", "DOUBLE", True, "Valor estimado para a área, sem arredondamento (pode ser fracionário)"),
    ("status_valor", "STRING", True, "OK, CONVERTIDO_TEXTO, VAZIO ou NAO_NUMERICO"),
    ("valor_texto_original", "STRING", True, "Conteúdo original quando a célula não era numérica"),
    ("linha_origem", "INT", True, "Linha da aba no Excel"),
    ("fonte_arquivo", "STRING", True, "Caminho do Excel na Bronze"),
    ("fonte_sha256", "STRING", False, "SHA-256 do arquivo Bronze processado"),
    ("id_ingestao_bronze", "STRING", True, "id_ingestao do manifesto Bronze"),
    ("id_execucao", "STRING", True, "Identificador desta execução do notebook 04"),
    ("data_processamento", "TIMESTAMP", True, "Data/hora do processamento (UTC)"),
]
colunas_ddl = ",\n    ".join(
    f"{nome} {tipo}{'' if nulo else ' NOT NULL'} COMMENT '{comentario}'" for nome, tipo, nulo, comentario in COLUNAS
)

# IF NOT EXISTS: se a tabela já existir, nada é alterado.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABELA_CENSO} (
    {colunas_ddl}
)
USING DELTA
COMMENT 'Projeto Saúde Perto SP - Silver: Censo 2022 estimado por área de abrangência de UBS (formato longo). Chave: cnes + aba_origem + variavel_origem.'
""")
print("Tabela disponível:", TABELA_CENSO)

# COMMAND ----------

# DBTITLE 1,7. Gravação idempotente
from pyspark.sql import functions as F

schema_valores = ("cnes STRING, cnes_original STRING, aba_origem STRING, variavel_origem STRING, valor DOUBLE, "
                  "status_valor STRING, valor_texto_original STRING, linha_origem INT")
schema_mapa = ("aba_origem STRING, variavel_origem STRING, variavel_dicionario STRING, grupo_tema STRING, "
               "tema STRING, descricao_variavel STRING, status_dicionario STRING")

# Valores (lista Python: None vira NULL) + descrição do dicionário por (aba, variável) + rastreabilidade
df_saida = (
    spark.createDataFrame(valores, schema_valores)
    .join(spark.createDataFrame(mapa_variaveis, schema_mapa), ["aba_origem", "variavel_origem"], "left")
    .withColumn("fonte_arquivo", F.lit(caminho_xlsx))
    .withColumn("fonte_sha256", F.lit(sha_atual))
    .withColumn("id_ingestao_bronze", F.lit(id_ingestao_bronze))
    .withColumn("id_execucao", F.lit(id_execucao))
    .withColumn("data_processamento", F.lit(data_execucao))
    .select(*[F.col(nome).cast(tipo) for nome, tipo, _, _ in COLUNAS])
)

# Conferir que o esquema da tabela existente coincide com o esperado
colunas_tabela = [c.name for c in spark.table(TABELA_CENSO).schema]
assert colunas_tabela == [n for n, _, _, _ in COLUNAS], "Esquema da tabela difere do esperado; gravação interrompida"

# Idempotência baseada no SHA-256 da fonte
estado = spark.sql(f"SELECT count(*) AS n, collect_set(fonte_sha256) AS versoes FROM {TABELA_CENSO}").first()

if estado["n"] == 0:
    df_saida.write.mode("append").saveAsTable(TABELA_CENSO)
    status_gravacao = "GRAVADO"
elif list(estado["versoes"]) == [sha_atual] and estado["n"] == len(valores):
    status_gravacao = "JA_PROCESSADO"
else:
    raise RuntimeError(
        f"{TABELA_CENSO} já contém {estado['n']} linha(s) de versão(ões) {list(estado['versoes'])}, "
        f"diferente do processamento atual ({len(valores)} linhas, SHA-256 {sha_atual}). "
        "Nada foi gravado; reprocessamento exige decisão explícita."
    )

print(f"Censo: {status_gravacao} | linhas preparadas: {len(valores)} | id_execucao = {id_execucao}")

# COMMAND ----------

# DBTITLE 1,8. Validação pós-gravação
# Conferência independente, lendo a tabela gravada
resumo = spark.sql(f"""
    SELECT count(*)                                         AS linhas,
           count(DISTINCT cnes, aba_origem, variavel_origem) AS chaves_distintas,
           count_if(cnes RLIKE '^[0-9]{{7}}$')              AS cnes_7_digitos,
           count(DISTINCT cnes)                             AS cnes_distintos,
           count_if(valor IS NULL)                          AS valores_nulos,
           count_if(status_valor IN ('VAZIO', 'NAO_NUMERICO')) AS status_sem_valor,
           count_if(valor <> floor(valor))                  AS valores_fracionarios,
           count(DISTINCT fonte_sha256)                     AS versoes_fonte,
           max(fonte_sha256)                                AS fonte_sha256
    FROM {TABELA_CENSO}
""").first()
print(resumo.asDict())

assert resumo["linhas"] == len(valores), f"Esperadas {len(valores)} linhas, encontradas {resumo['linhas']}"
assert resumo["chaves_distintas"] == resumo["linhas"], "Chave cnes + aba_origem + variavel_origem repetida"
assert resumo["cnes_7_digitos"] == resumo["linhas"], "Existe CNES fora do padrão de 7 dígitos"
assert resumo["cnes_distintos"] == QTD_AREAS, f"Esperados {QTD_AREAS} CNES distintos"
assert resumo["valores_nulos"] == resumo["status_sem_valor"], "Valor NULL sem status correspondente"
assert resumo["versoes_fonte"] == 1 and resumo["fonte_sha256"] == sha_atual, "Versão da fonte na tabela difere da Bronze"

# Por aba: CNES distintos e soma dos valores (comparada com a leitura do Excel)
por_aba = spark.sql(f"""
    SELECT aba_origem, count(DISTINCT cnes) AS cnes_distintos, count(DISTINCT variavel_origem) AS variaveis,
           count(*) AS valores, count_if(status_dicionario <> 'OK') AS valores_sem_descricao, sum(valor) AS soma_valores
    FROM {TABELA_CENSO} GROUP BY aba_origem ORDER BY aba_origem
""").toPandas()
soma_excel = df_valores.groupby("aba_origem").valor.sum()
for r in por_aba.itertuples():
    assert r.cnes_distintos == ABAS_TEMATICAS[r.aba_origem], f"{r.aba_origem}: {r.cnes_distintos} CNES"
    assert abs(r.soma_valores - soma_excel[r.aba_origem]) <= 1e-9 * max(1.0, abs(soma_excel[r.aba_origem])), \
        f"{r.aba_origem}: soma gravada difere da lida no Excel"
display(por_aba)

# Integridade com as tabelas Silver (somente leitura)
integ = spark.sql(f"""
    SELECT
      (SELECT count(DISTINCT c.cnes) FROM {TABELA_CENSO} c LEFT ANTI JOIN {TABELA_AREAS} a ON c.cnes = a.cnes)    AS cnes_sem_area,
      (SELECT count(DISTINCT c.cnes) FROM {TABELA_CENSO} c LEFT ANTI JOIN {TABELA_CADASTRO} u ON c.cnes = u.cnes) AS cnes_sem_cadastro,
      (SELECT count(*) FROM {TABELA_AREAS} a LEFT ANTI JOIN
          (SELECT DISTINCT cnes FROM {TABELA_CENSO} WHERE aba_origem = 'basico') b ON a.cnes = b.cnes)          AS areas_sem_basico,
      (SELECT count(*) FROM {TABELA_AREAS} a LEFT ANTI JOIN
          (SELECT DISTINCT cnes FROM {TABELA_CENSO} WHERE aba_origem = '{ABA_FCU}') f ON a.cnes = f.cnes)       AS areas_sem_fcu
""").first()
print(integ.asDict())
assert integ["cnes_sem_area"] == 0, "CNES do Censo sem área de abrangência"
assert integ["areas_sem_basico"] == 0, "Área de abrangência sem dados básicos do Censo"
assert integ["areas_sem_fcu"] == QTD_AREAS - ABAS_TEMATICAS[ABA_FCU], "Quantidade de áreas sem FCU diferente do esperado"

display(spark.sql(f"""
    SELECT status_dicionario, status_valor, count(*) AS qtd FROM {TABELA_CENSO}
    GROUP BY status_dicionario, status_valor ORDER BY status_dicionario, status_valor
"""))

display(spark.sql(f"""
    SELECT cnes, aba_origem, variavel_origem, variavel_dicionario, tema, descricao_variavel, valor
    FROM {TABELA_CENSO} WHERE cnes = (SELECT min(cnes) FROM {TABELA_CENSO}) AND aba_origem IN ('basico', '{ABA_FCU}')
    ORDER BY aba_origem, variavel_origem
"""))
print("Silver Censo: APROVADA")

# COMMAND ----------

# DBTITLE 1,Resumo e próximos passos
# MAGIC %md
# MAGIC ## Resultado esperado
# MAGIC * `ssp_silver_censo` com **652.230 linhas**: 1.332 variáveis × 485 áreas nas 9 abas completas, mais 15 variáveis × 414 áreas na FCU (1.347 variáveis no total). São 485 CNES distintos e a chave `cnes + aba_origem + variavel_origem` é única.
# MAGIC * 10 linhas `Total Geral` removidas. A soma de cada coluna confere com o total informado, exceto em 12 colunas não aditivas: média e desvio padrão de renda, percentuais da FCU e totais da FCU, que no Excel são municipais.
# MAGIC * 5 variáveis sem descrição no dicionário (`POP_NEGRAAAUBS`, `MNEGRAS_RESPDOMAAUBS`, `V004AA_FCU`, `V004AA` e `P_V004AA_FCU` da FCU), registradas como alerta.
# MAGIC * 71 áreas sem linhas de FCU: ausência preservada, sem zeros.
# MAGIC * Glossário `siglas_basico` (7 siglas) conferido com a documentação do notebook, sem gravação em tabela.
# MAGIC * Reexecução com a mesma versão da fonte: `status_gravacao = JA_PROCESSADO`, sem nova gravação.
# MAGIC
# MAGIC ## Consulta típica
# MAGIC Filtrar por `aba_origem` e `variavel_origem` (ou `variavel_dicionario`) e relacionar com `ssp_silver_areas_abrangencia` e `ssp_silver_ubs_cadastro` por `cnes`.