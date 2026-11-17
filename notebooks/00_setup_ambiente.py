# Databricks notebook source
# DBTITLE 1,Apresentação do notebook
# MAGIC %md
# MAGIC # 00 — Setup do ambiente | Projeto Saúde Perto SP
# MAGIC
# MAGIC **Responsabilidade:** Arquitetura e Engenharia de Dados (camadas Bronze e Silver). Sem camada Gold, indicadores ou dashboard.
# MAGIC
# MAGIC **Finalidade deste notebook**
# MAGIC 1. Instalar apenas as bibliotecas estritamente necessárias e compatíveis com o Databricks Serverless.
# MAGIC 2. Validar imports e versões.
# MAGIC 3. Verificar a existência e a integridade (tamanho e SHA-256) dos três arquivos originais, **somente leitura**.
# MAGIC 4. Verificar permissões no Unity Catalog antes de criar qualquer objeto.
# MAGIC 5. Criar (de forma não destrutiva, `IF NOT EXISTS`) o Volume que receberá a cópia bruta dos arquivos na camada Bronze.
# MAGIC
# MAGIC **Decisões de bibliotecas**
# MAGIC
# MAGIC | Biblioteca | Situação | Uso |
# MAGIC | --- | --- | --- |
# MAGIC | pandas, numpy, pyspark | Já presentes no runtime (não reinstalar) | Tratamento e gravação Delta |
# MAGIC | `xml.etree`, `zipfile`, `hashlib`, `re`, `html` | Biblioteca padrão do Python | Leitura de KML/KMZ (validada na inspeção) |
# MAGIC | openpyxl | Instalar | Leitura do XLSX com `pandas.read_excel` |
# MAGIC | shapely | Instalar | Geometrias, WKT, validade e `covers` (notebook 03) |
# MAGIC | pyproj | Instalar | Reprojeção para SIRGAS 2000 / UTM 23S (EPSG:31983) para cálculo de área em km² (notebook 03) |
# MAGIC | geopandas, pyogrio, fastkml | **Não instalar** | Não foram necessárias: o parser nativo (stdlib) já extraiu geometrias e atributos HTML na inspeção |
# MAGIC
# MAGIC **Nomenclatura no Unity Catalog** (o usuário não possui `CREATE SCHEMA` no catálogo `workspace`; o schema `workspace.default` permite `CREATE TABLE` e `CREATE VOLUME`):
# MAGIC * Volume bruto: `workspace.default.ssp_raw_files`
# MAGIC * Tabelas Bronze: `workspace.default.ssp_bronze_<objeto>`
# MAGIC * Tabelas Silver: `workspace.default.ssp_silver_<objeto>`
# MAGIC * Estrutura ideal (se um administrador conceder permissão): catálogo `saude_perto_sp` com schemas `bronze` e `silver`.

# COMMAND ----------

# DBTITLE 1,1. Instalação das bibliotecas necessárias
# MAGIC %pip install openpyxl==3.1.5 shapely==2.1.1 pyproj==3.7.1

# COMMAND ----------

# DBTITLE 1,2. Reinício do Python para carregar os pacotes
# Necessário para que os pacotes recém-instalados fiquem visíveis.
# Atenção: limpa variáveis da sessão; as células seguintes devem ser executadas depois deste passo.
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,3. Parâmetros do projeto
# Parâmetros centrais do projeto (repetidos no início de cada notebook para manter cada etapa independente).
PROJETO = "saude_perto_sp"
PREFIXO = "ssp"

# Unity Catalog (definido após verificação de permissões: só há CREATE TABLE/VOLUME em workspace.default)
CATALOGO = "workspace"
SCHEMA = "default"
VOLUME_RAW = f"{PREFIXO}_raw_files"
VOLUME_PATH = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME_RAW}"

# Arquivos originais (somente leitura; nunca são alterados)
DIR_ORIGEM = "/Workspace/Users/rm572201@fiap.com.br/Arquivos"
ARQUIVOS_ORIGEM = {
    "consulta_ubs_kml": f"{DIR_ORIGEM}/Consulta UBS.kml",
    "aaubs_2026_kmz": f"{DIR_ORIGEM}/Areas_de_Abrangencia_de_UBS_no_MSP_2026.kmz",
    "censo2022_aaubs_xlsx": f"{DIR_ORIGEM}/Dados_Estimados_Agregados_por_setores_UniversoCenso2022_MSP_SC_AAUBS2026_rev29052026.xlsx",
}

print(f"Volume bruto: {VOLUME_PATH}")
print(f"Padrão de tabelas: {CATALOGO}.{SCHEMA}.{PREFIXO}_bronze_* | {CATALOGO}.{SCHEMA}.{PREFIXO}_silver_*")

# COMMAND ----------

# DBTITLE 1,4. Validação de imports, versões e testes geoespaciais
import sys, platform, os, importlib, importlib.metadata as md

print(f"Python: {sys.version.split()[0]} | Arquitetura: {platform.machine()} | Runtime: {os.environ.get('DATABRICKS_RUNTIME_VERSION')}")

bibliotecas = {
    "pandas": "runtime", "numpy": "runtime", "pyspark": "runtime",
    "openpyxl": "instalada", "shapely": "instalada", "pyproj": "instalada",
    "xml.etree.ElementTree": "stdlib", "zipfile": "stdlib", "hashlib": "stdlib",
}
def identificar_versao(modulo, nome, origem):
    """Versão compatível com Serverless: prioriza __version__ do módulo
    (no Serverless o pyspark pode não estar registrado como distribuição 'pyspark')."""
    versao = getattr(modulo, "__version__", None)
    if versao:
        return str(versao)
    if origem == "stdlib":
        return f"stdlib (Python {sys.version.split()[0]})"
    try:
        return md.version(nome.split(".")[0])
    except md.PackageNotFoundError:
        return "desconhecida"

falhas = []
for nome, origem in bibliotecas.items():
    try:
        modulo = importlib.import_module(nome)
        print(f"OK    {nome:<24} {identificar_versao(modulo, nome, origem):<28} ({origem})")
    except Exception as e:
        falhas.append(nome)
        print(f"FALHA {nome:<24} {type(e).__name__}: {e}")

assert not falhas, f"Bibliotecas com falha: {falhas}"

# Testes funcionais do Shapely: covers x contains
# covers   -> True se o ponto está no interior OU sobre o limite (regra adotada no notebook 03)
# contains -> True somente se o ponto está no interior (limite não conta)
import pandas as pd
from shapely.geometry import Point, Polygon, MultiPolygon
from pyproj import CRS

exterior = [(0, 0), (10, 0), (10, 10), (0, 10)]
furo = [(4, 4), (6, 4), (6, 6), (4, 6)]
poligono = Polygon(exterior, holes=[furo])
assert poligono.is_valid, "Polígono de teste inválido"

# (caso, ponto, covers esperado, contains esperado)
casos = [
    ("interno",              Point(2, 2),  True,  True),
    ("borda externa",        Point(10, 5), True,  False),
    ("vértice",              Point(0, 0),  True,  False),
    ("externo",              Point(12, 5), False, False),
    ("dentro do furo",       Point(5, 5),  False, False),
    ("borda do furo",        Point(4, 5),  True,  False),
]

# MultiPolygon (as geometrias do KMZ são MultiGeometry): ponto interno à segunda parte
multi = MultiPolygon([poligono, Polygon([(20, 0), (25, 0), (25, 5), (20, 5)])])

resultados = []
for caso, ponto, esp_covers, esp_contains in casos:
    resultados.append(("Polygon com furo", caso, poligono.covers(ponto), esp_covers, poligono.contains(ponto), esp_contains))
ponto_multi = Point(22, 2)
resultados.append(("MultiPolygon", "interno à 2ª parte", multi.covers(ponto_multi), True, multi.contains(ponto_multi), True))

df_testes = pd.DataFrame(resultados, columns=["geometria", "caso", "covers", "covers_esperado", "contains", "contains_esperado"])
df_testes["aprovado"] = (df_testes.covers == df_testes.covers_esperado) & (df_testes.contains == df_testes.contains_esperado)
display(df_testes)
assert df_testes["aprovado"].all(), "Algum teste de covers/contains não retornou o resultado esperado"
print("Testes Shapely aprovados: covers inclui interior e limites; contains exclui limites; furos são respeitados.")

# Teste do pyproj: banco de dados PROJ disponível para os CRS usados no projeto
print("EPSG:4674 =", CRS.from_epsg(4674).name, "| EPSG:4326 =", CRS.from_epsg(4326).name, "| EPSG:31983 =", CRS.from_epsg(31983).name)

# COMMAND ----------

# DBTITLE 1,5. Verificação dos arquivos originais (somente leitura)
import hashlib, datetime
import pandas as pd

def sha256_arquivo(caminho, bloco=1024 * 1024):
    h = hashlib.sha256()
    with open(caminho, "rb") as f:  # leitura binária; o arquivo não é modificado
        for parte in iter(lambda: f.read(bloco), b""):
            h.update(parte)
    return h.hexdigest()

registros = []
for fonte, caminho in ARQUIVOS_ORIGEM.items():
    existe = os.path.isfile(caminho)
    registros.append({
        "fonte": fonte,
        "caminho": caminho,
        "existe": existe,
        "tamanho_bytes": os.path.getsize(caminho) if existe else None,
        "modificado_em": datetime.datetime.fromtimestamp(os.path.getmtime(caminho)).isoformat() if existe else None,
        "sha256": sha256_arquivo(caminho) if existe else None,
    })

df_arquivos = pd.DataFrame(registros)
display(df_arquivos)
assert df_arquivos["existe"].all(), "Há arquivos de origem ausentes"

# COMMAND ----------

# DBTITLE 1,6. Verificação de permissões no Unity Catalog
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
usuario = spark.sql("SELECT current_user()").first()[0]
workspace_id = w.get_workspace_id()
print("Usuário:", usuario, "| Workspace ID:", workspace_id)

print("\nCatálogos visíveis:")
display(spark.sql("SELECT catalog_name, catalog_owner FROM system.information_schema.catalogs ORDER BY 1"))

print(f"Schemas em {CATALOGO}:")
display(spark.sql(f"SELECT schema_name, schema_owner FROM {CATALOGO}.information_schema.schemata ORDER BY 1"))

# ---------------------------------------------------------------------------
# Privilégios efetivos do USUÁRIO ATUAL (somente leitura; nenhum GRANT é executado)
# Fonte: API oficial de permissões efetivas do Unity Catalog (grants.get_effective),
# que já considera concessões diretas, via grupos e herdadas do catálogo.
# Concessões de outros usuários/grupos não são retornadas para este principal.
# ---------------------------------------------------------------------------
import pandas as pd

def privilegios_efetivos(tipo, nome):
    resposta = w.grants.get_effective(tipo, nome, principal=usuario)
    linhas = []
    for atribuicao in resposta.privilege_assignments or []:
        for p in atribuicao.privileges or []:
            privilegio = p.privilege.value if hasattr(p.privilege, "value") else str(p.privilege)
            linhas.append({
                "nivel": tipo.upper(),
                "objeto": nome,
                "via_principal": atribuicao.principal,
                "privilegio": privilegio,
                "herdado_de": getattr(p, "inherited_from_name", None) or "-",
            })
    return linhas

colunas_priv = ["nivel", "objeto", "via_principal", "privilegio", "herdado_de"]
df_priv = pd.DataFrame(
    privilegios_efetivos("catalog", CATALOGO) + privilegios_efetivos("schema", f"{CATALOGO}.{SCHEMA}"),
    columns=colunas_priv,
)
if df_priv.empty:
    print("Nenhum privilégio efetivo retornado para o usuário atual.")
else:
    print("Privilégios efetivos do usuário atual (direto, via grupo ou herdados):")
    display(df_priv)

privs_cat = set(df_priv.loc[df_priv.nivel == "CATALOG", "privilegio"])
privs_sch = set(df_priv.loc[df_priv.nivel == "SCHEMA", "privilegio"])

# ---------------------------------------------------------------------------
# Propriedade (owner): a API de permissões efetivas não informa ownership.
# Os grupos de sistema do catálogo do workspace são identificados pelo ID do workspace
# e mapeados para os grupos locais equivalentes (verificação via is_member).
# ---------------------------------------------------------------------------
GRUPOS_SISTEMA = {
    f"_workspace_users_workspace_{workspace_id}": "users",
    f"_workspace_admins_workspace_{workspace_id}": "admins",
}
_cache_membro = {}

def usuario_pertence(principal):
    """True se o principal é o próprio usuário ou um grupo do qual ele participa."""
    if not principal:
        return False
    if principal == usuario:
        return True
    if principal not in _cache_membro:
        pertence = False
        if principal in GRUPOS_SISTEMA:
            pertence = bool(spark.sql("SELECT is_member(:g) AS r", args={"g": GRUPOS_SISTEMA[principal]}).first()["r"])
        else:
            # is_account_group_member: grupos de conta | is_member: grupos locais do workspace
            for funcao in ("is_account_group_member", "is_member"):
                try:
                    if spark.sql(f"SELECT {funcao}(:p) AS r", args={"p": principal}).first()["r"]:
                        pertence = True
                        break
                except Exception as e:
                    print(f"Aviso: {funcao}('{principal}') indisponível: {type(e).__name__}")
        _cache_membro[principal] = pertence
    return _cache_membro[principal]

# Propriedade: o owner possui todos os privilégios sobre o próprio objeto
dono_catalogo = spark.sql(
    "SELECT catalog_owner FROM system.information_schema.catalogs WHERE catalog_name = :c", args={"c": CATALOGO}
).first()["catalog_owner"]
dono_schema = spark.sql(
    f"SELECT schema_owner FROM {CATALOGO}.information_schema.schemata WHERE schema_name = :s", args={"s": SCHEMA}
).first()["schema_owner"]
eh_dono_catalogo = usuario_pertence(dono_catalogo)
eh_dono_schema = usuario_pertence(dono_schema)
print(f"Owner do catálogo: {dono_catalogo} (usuário é owner/membro: {eh_dono_catalogo})")
print(f"Owner do schema:   {dono_schema} (usuário é owner/membro: {eh_dono_schema})")

def tem_no_catalogo(priv):
    return eh_dono_catalogo or bool({priv, "ALL_PRIVILEGES"} & privs_cat)

def tem_no_schema(priv):
    # privilégios concedidos no catálogo são herdados pelos schemas
    return eh_dono_schema or tem_no_catalogo(priv) or bool({priv, "ALL_PRIVILEGES"} & privs_sch)

necessarios = {
    f"USE_CATALOG em {CATALOGO}": tem_no_catalogo("USE_CATALOG"),  # owner do schema ainda precisa de USE_CATALOG
    f"USE_SCHEMA em {CATALOGO}.{SCHEMA}": tem_no_schema("USE_SCHEMA"),
    f"CREATE_TABLE em {CATALOGO}.{SCHEMA}": tem_no_schema("CREATE_TABLE"),
    f"CREATE_VOLUME em {CATALOGO}.{SCHEMA}": tem_no_schema("CREATE_VOLUME"),
}
informativo = {f"CREATE_SCHEMA em {CATALOGO}": tem_no_catalogo("CREATE_SCHEMA")}

df_checagem = pd.DataFrame(
    [{"verificacao": k, "tipo": "obrigatória", "status": "OK" if v else "AUSENTE"} for k, v in necessarios.items()]
    + [{"verificacao": k, "tipo": "informativa", "status": "OK" if v else "AUSENTE"} for k, v in informativo.items()]
)
display(df_checagem)

print("Observação: privilégios de administrador do metastore não são avaliados aqui.")
print(f"Nomenclatura definida para o projeto: {CATALOGO}.{SCHEMA} com prefixos {PREFIXO}_bronze_* e {PREFIXO}_silver_*.")
assert all(necessarios.values()), "Permissões mínimas ausentes para o usuário atual; solicite ao administrador antes de continuar"
print("Permissões mínimas: APROVADAS")

# COMMAND ----------

# DBTITLE 1,7. Criação não destrutiva do Volume bruto
# Criação segura e idempotente do Volume bruto.
# Nenhum conteúdo é apagado, movido ou sobrescrito nesta célula.

def consultar_volume():
    linhas = spark.sql(
        f"""SELECT volume_name, volume_type, volume_owner, storage_location, comment
            FROM {CATALOGO}.information_schema.volumes
            WHERE volume_catalog = :c AND volume_schema = :s AND volume_name = :v""",
        args={"c": CATALOGO, "s": SCHEMA, "v": VOLUME_RAW},
    ).collect()
    return linhas[0] if linhas else None

volume_existente = consultar_volume()

if volume_existente is None:
    print(f"Volume {CATALOGO}.{SCHEMA}.{VOLUME_RAW} não existe; criando como MANAGED.")
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.{VOLUME_RAW}
        COMMENT 'Projeto Saúde Perto SP - cópias imutáveis dos arquivos originais (camada Bronze)'
    """)
    volume_info = consultar_volume()
else:
    print(f"Volume {CATALOGO}.{SCHEMA}.{VOLUME_RAW} já existe; nenhuma criação ou alteração foi feita.")
    volume_info = volume_existente

assert volume_info is not None, "Volume não encontrado após a criação"
print(f"Tipo: {volume_info.volume_type} | Owner: {volume_info.volume_owner} | Comentário: {volume_info.comment}")

# A camada Bronze foi desenhada para um Volume gerenciado; um Volume externo com o mesmo nome interrompe o setup.
assert volume_info.volume_type == "MANAGED", (
    f"O Volume existente é {volume_info.volume_type}, não MANAGED. Setup interrompido sem alterações; "
    "revise o nome do Volume ou a estrutura antes de continuar."
)

# Inventário do conteúdo atual (somente leitura)
arquivos_existentes = [
    os.path.join(raiz, nome) for raiz, _, nomes in os.walk(VOLUME_PATH) for nome in nomes
]
if arquivos_existentes:
    print(f"ALERTA: o Volume não está vazio ({len(arquivos_existentes)} arquivo(s)). "
          "Nada foi apagado ou sobrescrito; o notebook 01 não substituirá arquivos existentes.")
    for caminho in arquivos_existentes[:20]:
        print("  -", caminho)
else:
    print("Volume vazio: pronto para a ingestão Bronze.")

# COMMAND ----------

# DBTITLE 1,Resumo e próximos passos
# MAGIC %md
# MAGIC ## Resultado esperado
# MAGIC * Todas as bibliotecas com status **OK** e os 7 testes de `covers`/`contains` com `aprovado = true`.
# MAGIC * Os três arquivos com `existe = true`, tamanho e SHA-256 registrados (os hashes serão comparados com as cópias no notebook 01).
# MAGIC * Permissões mínimas do usuário atual **OK**; `CREATE_SCHEMA` ausente apenas como informação.
# MAGIC * Volume `workspace.default.ssp_raw_files` do tipo `MANAGED` e vazio (ou alerta listando o conteúdo existente, sem remoção).
# MAGIC
# MAGIC ## Próximo notebook
# MAGIC `01_bronze_ingestao`: cópia dos arquivos para o Volume (sem sobrescrever), registro de rastreabilidade e tabelas Bronze com o conteúdo bruto do KML, KMZ e das 14 abas.