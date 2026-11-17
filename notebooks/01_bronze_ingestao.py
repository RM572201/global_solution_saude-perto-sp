# Databricks notebook source
# DBTITLE 1,Apresentação do notebook
# MAGIC %md
# MAGIC # 01 — Ingestão Bronze | Projeto Saúde Perto SP
# MAGIC
# MAGIC **Objetivo:** preservar e registrar os três arquivos originais na camada Bronze, sem nenhuma transformação de conteúdo. O parsing de KML, KMZ e XLSX será feito nos notebooks 02 a 04.
# MAGIC
# MAGIC **Pré-requisito:** `00_setup_ambiente` aprovado (Volume `workspace.default.ssp_raw_files` existente e MANAGED). Este notebook usa apenas biblioteca padrão do Python, pandas e PySpark; não há instalação de pacotes.
# MAGIC
# MAGIC **Organização no Volume** (nome original do arquivo preservado):
# MAGIC
# MAGIC | Fonte | Caminho Bronze |
# MAGIC | --- | --- |
# MAGIC | `consulta_ubs_kml` | `/Volumes/workspace/default/ssp_raw_files/consulta_ubs_kml/Consulta UBS.kml` |
# MAGIC | `aaubs_2026_kmz` | `/Volumes/workspace/default/ssp_raw_files/aaubs_2026_kmz/Areas_de_Abrangencia_de_UBS_no_MSP_2026.kmz` |
# MAGIC | `censo2022_aaubs_xlsx` | `/Volumes/workspace/default/ssp_raw_files/censo2022_aaubs_xlsx/Dados_Estimados_..._rev29052026.xlsx` |
# MAGIC
# MAGIC **Regras de idempotência**
# MAGIC
# MAGIC | Situação do destino | Ação | Status no manifesto |
# MAGIC | --- | --- | --- |
# MAGIC | Não existe | Copia, confere SHA-256 da cópia | `INGERIDO` |
# MAGIC | Existe com o mesmo SHA-256 | Não copia | `JA_INGERIDO` |
# MAGIC | Existe com SHA-256 diferente | **Interrompe tudo antes de qualquer cópia ou gravação** e exibe a divergência | (nada gravado) |
# MAGIC
# MAGIC **Tabela de controle:** `workspace.default.ssp_bronze_manifest` (Delta, somente *append*). Cada execução grava uma linha por arquivo com o mesmo `id_ingestao`, funcionando como log auditável.
# MAGIC
# MAGIC **Garantias:** os arquivos de origem são apenas lidos; nenhum arquivo do Volume é sobrescrito, movido ou apagado; o manifesto nunca é sobrescrito.

# COMMAND ----------

# DBTITLE 1,1. Parâmetros do projeto
# Parâmetros (mesmos valores do 00_setup_ambiente)
PREFIXO = "ssp"
CATALOGO = "workspace"
SCHEMA = "default"
VOLUME_RAW = f"{PREFIXO}_raw_files"
VOLUME_PATH = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME_RAW}"
TABELA_MANIFESTO = f"{CATALOGO}.{SCHEMA}.{PREFIXO}_bronze_manifest"

DIR_ORIGEM = "/Workspace/Users/rm572201@fiap.com.br/Arquivos"
ARQUIVOS_ORIGEM = {
    "consulta_ubs_kml": f"{DIR_ORIGEM}/Consulta UBS.kml",
    "aaubs_2026_kmz": f"{DIR_ORIGEM}/Areas_de_Abrangencia_de_UBS_no_MSP_2026.kmz",
    "censo2022_aaubs_xlsx": f"{DIR_ORIGEM}/Dados_Estimados_Agregados_por_setores_UniversoCenso2022_MSP_SC_AAUBS2026_rev29052026.xlsx",
}

# SHA-256 registrados na execução aprovada do 00_setup_ambiente (16/11/2026).
# Se a origem mudar, o processo é interrompido para decisão explícita (nova versão do arquivo).
VALIDAR_HASH_ESPERADO = True
HASH_ESPERADO = {
    "consulta_ubs_kml": "5fbf22474918ecdf81536b28fa2e0f8dc6bd2f3f6250c015a3174f04b73430d1",
    "aaubs_2026_kmz": "3884c6032114b9734edd4ac81a71ad95fa23dc52376d8283e7c71f125331113e",
    "censo2022_aaubs_xlsx": "b5332669c1dc776589dae4aa00a3eb574cca047aa16462cdc1814f76325ef723",
}

print("Volume:", VOLUME_PATH)
print("Manifesto:", TABELA_MANIFESTO)

# COMMAND ----------

# DBTITLE 1,2. Funções auxiliares (somente leitura)
import os, hashlib, shutil, uuid, datetime
import pandas as pd

def sha256_arquivo(caminho, bloco=1024 * 1024):
    """SHA-256 por leitura binária em blocos; não altera o arquivo."""
    h = hashlib.sha256()
    with open(caminho, "rb") as f:
        for parte in iter(lambda: f.read(bloco), b""):
            h.update(parte)
    return h.hexdigest()

def caminho_bronze(fonte, caminho_origem):
    """Destino no Volume: <volume>/<fonte>/<nome original do arquivo>."""
    return os.path.join(VOLUME_PATH, fonte, os.path.basename(caminho_origem))

def extensao(caminho):
    return os.path.splitext(caminho)[1].lower().lstrip(".")

def agora_utc():
    return datetime.datetime.now(datetime.timezone.utc)

def notebook_atual():
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    except Exception:
        return None

# COMMAND ----------

# DBTITLE 1,3. Pré-verificação: Volume, origem e plano de ingestão
# Nenhuma escrita ocorre nesta célula. Ela monta o plano e interrompe se houver divergência.

# 3.1 Volume existente e MANAGED
vol = spark.sql(
    f"""SELECT volume_type FROM {CATALOGO}.information_schema.volumes
        WHERE volume_catalog = :c AND volume_schema = :s AND volume_name = :v""",
    args={"c": CATALOGO, "s": SCHEMA, "v": VOLUME_RAW},
).collect()
assert vol, f"Volume {CATALOGO}.{SCHEMA}.{VOLUME_RAW} não existe; execute o 00_setup_ambiente"
assert vol[0]["volume_type"] == "MANAGED", f"Volume não é MANAGED: {vol[0]['volume_type']}"

# 3.2 Plano por arquivo
plano = []
for fonte, origem in ARQUIVOS_ORIGEM.items():
    assert os.path.isfile(origem), f"Arquivo de origem ausente: {origem}"
    hash_origem = sha256_arquivo(origem)
    destino = caminho_bronze(fonte, origem)
    hash_destino = sha256_arquivo(destino) if os.path.isfile(destino) else None

    if hash_destino is None:
        acao = "COPIAR"
    elif hash_destino == hash_origem:
        acao = "JA_INGERIDO"
    else:
        acao = "DIVERGENTE"

    plano.append({
        "fonte": fonte,
        "caminho_origem": origem,
        "caminho_bronze": destino,
        "tamanho_origem": os.path.getsize(origem),
        "hash_origem": hash_origem,
        "hash_esperado": HASH_ESPERADO.get(fonte),
        "hash_destino": hash_destino,
        "acao": acao,
    })

df_plano = pd.DataFrame(plano)
df_plano["origem_confere_esperado"] = df_plano.hash_origem == df_plano.hash_esperado
display(df_plano)

# 3.3 Bloqueios (antes de qualquer cópia ou gravação)
if VALIDAR_HASH_ESPERADO and not df_plano.origem_confere_esperado.all():
    raise RuntimeError(
        "Arquivo de origem difere do hash registrado no 00_setup_ambiente: "
        f"{df_plano.loc[~df_plano.origem_confere_esperado, 'fonte'].tolist()}. Nada foi copiado ou gravado."
    )

divergentes = df_plano[df_plano.acao == "DIVERGENTE"]
if not divergentes.empty:
    display(divergentes[["fonte", "caminho_bronze", "hash_origem", "hash_destino"]])
    raise RuntimeError(
        "Arquivo já existe no Volume com SHA-256 diferente da origem. "
        "Processo interrompido; nenhum arquivo foi copiado, sobrescrito ou gravado no manifesto."
    )

print("Plano aprovado:", df_plano.acao.value_counts().to_dict())

# COMMAND ----------

# DBTITLE 1,4. Criação não destrutiva da tabela de manifesto
# IF NOT EXISTS: se a tabela já existir, nada é alterado.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABELA_MANIFESTO} (
    id_ingestao              STRING    NOT NULL COMMENT 'Identificador único da execução do notebook 01',
    fonte                    STRING    NOT NULL COMMENT 'Identificador lógico da fonte',
    nome_arquivo             STRING    COMMENT 'Nome original do arquivo',
    extensao                 STRING    COMMENT 'Extensão do arquivo (kml, kmz, xlsx)',
    tamanho_bytes            BIGINT    COMMENT 'Tamanho do arquivo em bytes',
    sha256                   STRING    NOT NULL COMMENT 'SHA-256 do arquivo de origem (igual ao da cópia Bronze)',
    caminho_origem           STRING    COMMENT 'Caminho do arquivo original no Workspace',
    caminho_bronze           STRING    COMMENT 'Caminho da cópia imutável no Volume',
    data_modificacao_origem  TIMESTAMP COMMENT 'Data de modificação do arquivo de origem (UTC)',
    data_ingestao            TIMESTAMP COMMENT 'Data/hora da execução (UTC)',
    status                   STRING    NOT NULL COMMENT 'INGERIDO ou JA_INGERIDO',
    usuario                  STRING    COMMENT 'Usuário que executou',
    notebook                 STRING    COMMENT 'Notebook que executou a ingestão',
    observacao               STRING    COMMENT 'Observações da ingestão'
)
USING DELTA
COMMENT 'Projeto Saúde Perto SP - manifesto de ingestão Bronze (append-only)'
""")

display(spark.sql(f"DESCRIBE TABLE {TABELA_MANIFESTO}"))

# COMMAND ----------

# DBTITLE 1,5. Cópia dos arquivos para o Volume com conferência de hash
id_ingestao = str(uuid.uuid4())
data_ingestao = agora_utc()
usuario = spark.sql("SELECT current_user()").first()[0]
notebook = notebook_atual()

# Registros anteriores no manifesto (para observação dos casos JA_INGERIDO)
hashes_registrados = {
    r["sha256"] for r in spark.sql(f"SELECT DISTINCT sha256 FROM {TABELA_MANIFESTO} WHERE status = 'INGERIDO'").collect()
}

registros = []
for item in plano:
    origem, destino, fonte = item["caminho_origem"], item["caminho_bronze"], item["fonte"]

    if item["acao"] == "COPIAR":
        # Garantia contra sobrescrita: o destino não pode ter surgido após a pré-verificação
        assert not os.path.exists(destino), f"Destino surgiu após a pré-verificação: {destino}"
        os.makedirs(os.path.dirname(destino), exist_ok=True)
        shutil.copyfile(origem, destino)  # copia apenas bytes; a origem é somente lida
        hash_copia = sha256_arquivo(destino)
        if hash_copia != item["hash_origem"]:
            raise RuntimeError(
                f"Hash da cópia difere da origem para {fonte}. A cópia foi mantida para inspeção "
                f"(não apagada) em {destino}; nada foi gravado no manifesto."
            )
        status, obs = "INGERIDO", "Cópia criada e SHA-256 conferido"
    else:  # JA_INGERIDO
        status = "JA_INGERIDO"
        obs = ("Arquivo já presente com o mesmo SHA-256; nenhuma cópia realizada"
               + ("" if item["hash_origem"] in hashes_registrados else "; sem registro INGERIDO anterior no manifesto"))

    # A origem não pode ter mudado durante o processo
    assert sha256_arquivo(origem) == item["hash_origem"], f"Origem alterada durante a ingestão: {origem}"

    registros.append((
        id_ingestao, fonte, os.path.basename(origem), extensao(origem), int(os.path.getsize(origem)),
        item["hash_origem"], origem, destino,
        datetime.datetime.fromtimestamp(os.path.getmtime(origem), tz=datetime.timezone.utc),
        data_ingestao, status, usuario, notebook, obs,
    ))

print(f"id_ingestao = {id_ingestao}")
for r in registros:
    print(f"{r[10]:<12} {r[1]:<22} {r[4]:>10} bytes  {r[5][:12]}...")

# COMMAND ----------

# DBTITLE 1,6. Registro no manifesto (append)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

schema_manifesto = StructType([
    StructField("id_ingestao", StringType(), False),
    StructField("fonte", StringType(), False),
    StructField("nome_arquivo", StringType(), True),
    StructField("extensao", StringType(), True),
    StructField("tamanho_bytes", LongType(), True),
    StructField("sha256", StringType(), False),
    StructField("caminho_origem", StringType(), True),
    StructField("caminho_bronze", StringType(), True),
    StructField("data_modificacao_origem", TimestampType(), True),
    StructField("data_ingestao", TimestampType(), True),
    StructField("status", StringType(), False),
    StructField("usuario", StringType(), True),
    StructField("notebook", StringType(), True),
    StructField("observacao", StringType(), True),
])

assert len(registros) == len(ARQUIVOS_ORIGEM), "Quantidade de registros diferente da quantidade de fontes"
df_manifesto = spark.createDataFrame(registros, schema=schema_manifesto)

# append: nunca sobrescreve registros anteriores
df_manifesto.write.mode("append").saveAsTable(TABELA_MANIFESTO)
print(f"{len(registros)} registro(s) adicionados a {TABELA_MANIFESTO}")

# COMMAND ----------

# DBTITLE 1,7. Validação pós-ingestão
# Conferência independente: lê o manifesto gravado e recalcula os hashes das cópias no Volume
gravados = spark.sql(
    f"""SELECT fonte, nome_arquivo, extensao, tamanho_bytes, sha256, caminho_bronze, status, data_ingestao
        FROM {TABELA_MANIFESTO} WHERE id_ingestao = :id ORDER BY fonte""",
    args={"id": id_ingestao},
).toPandas()

assert len(gravados) == len(ARQUIVOS_ORIGEM), f"Esperados {len(ARQUIVOS_ORIGEM)} registros, encontrados {len(gravados)}"

gravados["hash_volume"] = gravados.caminho_bronze.apply(sha256_arquivo)
gravados["tamanho_volume"] = gravados.caminho_bronze.apply(os.path.getsize)
gravados["hash_confere"] = gravados.hash_volume == gravados.sha256
gravados["tamanho_confere"] = gravados.tamanho_volume == gravados.tamanho_bytes
display(gravados)

assert gravados.hash_confere.all() and gravados.tamanho_confere.all(), "Divergência entre manifesto e Volume"

print("Conteúdo do Volume:")
for raiz, _, nomes in os.walk(VOLUME_PATH):
    for nome in nomes:
        print("  -", os.path.join(raiz, nome))

print("\nHistórico do manifesto (todas as execuções):")
display(spark.sql(
    f"SELECT id_ingestao, status, count(*) AS arquivos, max(data_ingestao) AS data_ingestao "
    f"FROM {TABELA_MANIFESTO} GROUP BY id_ingestao, status ORDER BY data_ingestao"
))
print("Ingestão Bronze: APROVADA")

# COMMAND ----------

# DBTITLE 1,Resumo e próximos passos
# MAGIC %md
# MAGIC ## Resultado esperado
# MAGIC * **1ª execução:** plano com 3 ações `COPIAR`; 3 registros `INGERIDO` no manifesto; hashes do Volume iguais aos da origem.
# MAGIC * **Reexecuções:** plano com 3 ações `JA_INGERIDO`; nenhuma cópia; 3 novos registros `JA_INGERIDO` (log de auditoria).
# MAGIC * **Divergência:** erro antes de qualquer escrita, com a tabela de hashes exibida.
# MAGIC
# MAGIC ## Próximo notebook
# MAGIC `02_silver_ubs_cadastro`: leitura do KML a partir da cópia Bronze no Volume e estruturação do cadastro das UBS.