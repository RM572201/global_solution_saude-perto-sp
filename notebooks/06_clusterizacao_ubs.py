# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Introdução
# MAGIC %md
# MAGIC # Clusterização das UBS | Projeto Saúde Perto SP
# MAGIC
# MAGIC **Objetivo:** agrupar as 485 UBS do Município de São Paulo em perfis territoriais semelhantes, a partir dos indicadores do Censo 2022 estimados por área de abrangência.
# MAGIC
# MAGIC **Fonte:** `workspace.default.ssp_gold_ubs_indicadores` (1 linha por CNES).
# MAGIC
# MAGIC **Método:** KMeans (scikit-learn) com pré-processamento no mesmo `Pipeline` (imputação, `log1p` nas variáveis de cauda longa, padronização). Escolha de k por silhouette, Davies-Bouldin, cotovelo e interpretabilidade. Modelo registrado no MLflow.
# MAGIC
# MAGIC **Decisões:**
# MAGIC * Só taxas, percentuais, densidade e renda média entram no modelo. Contagens absolutas (ex.: total de pessoas) foram excluídas porque medem o tamanho da área, não o perfil.
# MAGIC * `cnes`, nomes, endereço e coordenadas são identificadores; `crs` e `sts` são recortes administrativos e servem só para interpretar os clusters.
# MAGIC * `pct_pessoas_fcu` é NULL em 71 áreas sem registro de favela/comunidade urbana na fonte. Nesta análise, NULL é tratado como 0% (premissa: ausência de registro = ausência de FCU).

# COMMAND ----------

# DBTITLE 1,Imports e leitura da Gold
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import mlflow
from mlflow.models import infer_signature
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, FunctionTransformer
from sklearn.impute import SimpleImputer
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score, davies_bouldin_score, adjusted_rand_score

SEED = 42

# Features lineares (percentuais em escala 0-100, sem cauda muito longa)
FEATS_LIN = [
    "pct_60_anos_ou_mais",
    "pct_0_a_4_anos",
    "taxa_analfabetismo_15_anos_ou_mais",
    "pct_pretos_pardos",
    "pct_unidades_domesticas_unipessoais",
    "pct_pessoas_fcu",
]
# Features de cauda longa: recebem log1p antes da padronização
FEATS_LOG = [
    "pct_domicilios_esgoto_nao_rede_geral",
    "densidade_demografica_hab_km2",
    "renda_media_responsavel",
]
FEATS = FEATS_LIN + FEATS_LOG

pdf = spark.sql("""
    SELECT cnes, nome_ubs, crs, sts, latitude, longitude,
           pct_60_anos_ou_mais, pct_0_a_4_anos, taxa_analfabetismo_15_anos_ou_mais,
           pct_pretos_pardos, pct_unidades_domesticas_unipessoais,
           P_V001AA_FCU            AS pct_pessoas_fcu,
           pct_domicilios_esgoto_nao_rede_geral,
           densidade_demografica_hab_km2,
           MEAN_V06004AAUBS2       AS renda_media_responsavel
    FROM workspace.default.ssp_gold_ubs_indicadores
""").toPandas()

print("Shape:", pdf.shape, "| CNES distintos:", pdf["cnes"].nunique())
print("Nulos por feature:\n", pdf[FEATS].isna().sum()[lambda s: s > 0])
display(pdf.head())

# COMMAND ----------

# DBTITLE 1,EDA: tipos, estatísticas e correlograma
tipos = pd.DataFrame({
    "feature": FEATS,
    "tipo": ["numérica contínua (percentual)"] * 6 + ["numérica contínua (percentual, cauda longa)",
                                                     "numérica contínua (hab/km², cauda longa)",
                                                     "numérica contínua (R$, cauda longa)"],
})
display(tipos)
display(pdf[FEATS].describe().T.round(2).reset_index())

corr = pdf[FEATS].corr()
fig, ax = plt.subplots(figsize=(9, 7))
im = ax.imshow(corr, cmap="RdBu_r", vmin=-1, vmax=1)
ax.set_xticks(range(len(FEATS)), FEATS, rotation=60, ha="right", fontsize=8)
ax.set_yticks(range(len(FEATS)), FEATS, fontsize=8)
for i in range(len(FEATS)):
    for j in range(len(FEATS)):
        ax.text(j, i, f"{corr.iloc[i, j]:.2f}", ha="center", va="center", fontsize=7)
fig.colorbar(im)
ax.set_title("Correlograma (Pearson) dos indicadores por UBS")
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Pipeline de pré-processamento
def build_preprocessor():
    return ColumnTransformer([
        # NULL em pct_pessoas_fcu = sem registro de FCU -> 0% (premissa documentada)
        ("lin", Pipeline([
            ("imp", SimpleImputer(strategy="constant", fill_value=0)),
            ("sc", StandardScaler()),
        ]), FEATS_LIN),
        ("log", Pipeline([
            ("imp", SimpleImputer(strategy="median")),
            ("log", FunctionTransformer(np.log1p, feature_names_out="one-to-one")),
            ("sc", StandardScaler()),
        ]), FEATS_LOG),
    ])

X = pdf[FEATS]
Z = build_preprocessor().fit_transform(X)
print("Matriz pré-processada:", Z.shape)

# COMMAND ----------

# DBTITLE 1,Escolha de k (2 a 10)
resultados = []
for k in range(2, 11):
    km = KMeans(n_clusters=k, n_init=10, random_state=SEED).fit(Z)
    resultados.append({
        "k": k,
        "silhouette": silhouette_score(Z, km.labels_),
        "davies_bouldin": davies_bouldin_score(Z, km.labels_),
        "inercia": km.inertia_,
        "menor_cluster": np.bincount(km.labels_).min(),
    })
res_k = pd.DataFrame(resultados)
display(res_k.round(3))

fig, axes = plt.subplots(1, 2, figsize=(11, 4))
axes[0].plot(res_k.k, res_k.inercia, "o-"); axes[0].set_title("Cotovelo (inércia)"); axes[0].set_xlabel("k")
axes[1].plot(res_k.k, res_k.silhouette, "o-", color="tab:orange"); axes[1].set_title("Silhouette"); axes[1].set_xlabel("k")
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Decisão sobre k
# MAGIC %md
# MAGIC ### Decisão sobre k
# MAGIC A melhor silhouette é em k = 2, mas essa divisão só separa "centro expandido" de "periferia" e serve pouco para o planejamento. **Adotamos k = 4**: a silhouette continua razoável, o Davies-Bouldin é parecido com o de k = 2 a 3, o cotovelo fica entre 3 e 4, e os quatro grupos têm leitura territorial clara (perfis na próxima seção). O menor grupo tem 19 UBS (≈ 4%), acima do limite de 1%.

# COMMAND ----------

# DBTITLE 1,Modelo final (k = 4) e estabilidade
K = 4

def build_model(seed=SEED):
    return Pipeline([
        ("pre", build_preprocessor()),
        ("kmeans", KMeans(n_clusters=K, n_init=10, random_state=seed)),
    ])

model = build_model().fit(X)
labels = model.predict(X)
Zf = model.named_steps["pre"].transform(X)

sil = silhouette_score(Zf, labels)
db = davies_bouldin_score(Zf, labels)
ari_runs = [adjusted_rand_score(labels, build_model(s).fit_predict(X)) for s in (7, 123)]

print(f"Silhouette: {sil:.3f} | Davies-Bouldin: {db:.3f}")
print(f"Estabilidade (ARI vs. seeds 7 e 123): {ari_runs[0]:.3f}, {ari_runs[1]:.3f}")
print("Tamanhos:", dict(zip(*np.unique(labels, return_counts=True))))

# COMMAND ----------

# DBTITLE 1,Perfil e nome dos clusters
pdf["cluster"] = labels
perfil = pdf.groupby("cluster")[FEATS].median()

# Nomeia os clusters por regra sobre as medianas (não depende do número do cluster)
nomes, restantes = {}, list(perfil.index)
c = perfil.loc[restantes, "renda_media_responsavel"].idxmax(); nomes[c] = "Consolidado de alta renda"; restantes.remove(c)
c = perfil.loc[restantes, "pct_domicilios_esgoto_nao_rede_geral"].idxmax(); nomes[c] = "Periferia de baixa densidade e sem saneamento"; restantes.remove(c)
c = perfil.loc[restantes, "pct_pessoas_fcu"].idxmax(); nomes[c] = "Urbano vulnerável (favelas e comunidades)"; restantes.remove(c)
nomes[restantes[0]] = "Urbano intermediário"

pdf["perfil"] = pdf["cluster"].map(nomes)

tabela_perfil = (pdf.groupby(["cluster", "perfil"])
                   .agg(qtd_ubs=("cnes", "count"), **{f: (f, "median") for f in FEATS})
                   .round(1).reset_index())
display(tabela_perfil)
display(pd.crosstab(pdf["perfil"], pdf["crs"], margins=True).reset_index())

# COMMAND ----------

# DBTITLE 1,Projeção PCA 2D e mapa
pca = PCA(n_components=2, random_state=SEED).fit(Zf)
P = pca.transform(Zf)

fig, axes = plt.subplots(1, 2, figsize=(14, 6))
for c in sorted(nomes):
    m = labels == c
    axes[0].scatter(P[m, 0], P[m, 1], s=14, label=nomes[c])
    axes[1].scatter(pdf.longitude[m], pdf.latitude[m], s=14, label=nomes[c])
axes[0].set_title(f"PCA 2D (variância explicada: {pca.explained_variance_ratio_.sum():.0%})")
axes[0].set_xlabel("PC1"); axes[0].set_ylabel("PC2")
axes[1].set_title("Localização das UBS por perfil")
axes[1].set_xlabel("Longitude"); axes[1].set_ylabel("Latitude"); axes[1].set_aspect("equal")
axes[1].legend(fontsize=8, loc="lower left")
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Gráficos: tamanho e assinatura dos perfis
# Paleta fixa por perfil, reutilizada em todos os gráficos abaixo
ORDEM_PERFIS = ["Consolidado de alta renda", "Urbano intermediário",
                "Urbano vulnerável (favelas e comunidades)", "Periferia de baixa densidade e sem saneamento"]
CORES = dict(zip(ORDEM_PERFIS, ["#2b83ba", "#abdda4", "#fdae61", "#d7191c"]))
ROTULOS = {
    "pct_60_anos_ou_mais": "% 60+ anos", "pct_0_a_4_anos": "% 0-4 anos",
    "taxa_analfabetismo_15_anos_ou_mais": "Analfabetismo 15+", "pct_pretos_pardos": "% pretos e pardos",
    "pct_unidades_domesticas_unipessoais": "% unipessoais", "pct_pessoas_fcu": "% pessoas em FCU",
    "pct_domicilios_esgoto_nao_rede_geral": "% esgoto fora rede",
    "densidade_demografica_hab_km2": "Densidade (hab/km²)", "renda_media_responsavel": "Renda média",
}
pdf["perfil"] = pdf["cluster"].map(nomes)

fig, axes = plt.subplots(1, 2, figsize=(17, 5.5), gridspec_kw={"width_ratios": [1, 2]})

# 1) Quantidade de UBS por perfil
qtd = pdf.perfil.value_counts().reindex(ORDEM_PERFIS)
barras = axes[0].barh(qtd.index, qtd.values, color=[CORES[p] for p in qtd.index])
axes[0].bar_label(barras, labels=[f"{v} ({v / qtd.sum():.0%})" for v in qtd.values], padding=3)
axes[0].invert_yaxis(); axes[0].set_xlim(0, qtd.max() * 1.25)
axes[0].set_title("UBS por perfil"); axes[0].set_xlabel("Quantidade de UBS")

# 2) Assinatura dos perfis: mediana de cada indicador em desvios-padrão em relação à média da cidade
z = pd.DataFrame(Zf, columns=FEATS)
z["perfil"] = pdf.perfil.values
assinatura = z.groupby("perfil")[FEATS].median().reindex(ORDEM_PERFIS)
lim = np.abs(assinatura.values).max()
im = axes[1].imshow(assinatura, cmap="RdBu_r", vmin=-lim, vmax=lim, aspect="auto")
axes[1].set_xticks(range(len(FEATS)), [ROTULOS[f] for f in FEATS], rotation=35, ha="right")
axes[1].set_yticks(range(len(ORDEM_PERFIS)), ORDEM_PERFIS)
for i in range(assinatura.shape[0]):
    for j in range(assinatura.shape[1]):
        axes[1].text(j, i, f"{assinatura.iloc[i, j]:+.1f}", ha="center", va="center", fontsize=9)
fig.colorbar(im, ax=axes[1], label="desvios-padrão vs. média da cidade")
axes[1].set_title("Assinatura dos perfis (mediana padronizada; renda, densidade e esgoto em escala log)")
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Gráficos: distribuição dos indicadores por perfil
fig, axes = plt.subplots(3, 3, figsize=(17, 13))
for ax, f in zip(axes.ravel(), FEATS):
    dados = [pdf.loc[pdf.perfil == p, f].dropna() for p in ORDEM_PERFIS]
    bp = ax.boxplot(dados, patch_artist=True, widths=0.6, showfliers=True,
                    flierprops={"markersize": 3, "alpha": 0.5})
    for caixa, p in zip(bp["boxes"], ORDEM_PERFIS):
        caixa.set_facecolor(CORES[p])
    if f in ("densidade_demografica_hab_km2", "renda_media_responsavel"):
        ax.set_yscale("log")
    ax.set_xticks(range(1, 5), ["Alta renda", "Intermediário", "Vulnerável", "Periferia s/ san."], fontsize=9)
    ax.set_title(ROTULOS[f])
    ax.grid(axis="y", alpha=0.3)
fig.suptitle("Distribuição dos indicadores por perfil (escala log em densidade e renda)", fontsize=14)
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Gráficos: perfis por CRS
tab = pd.crosstab(pdf.crs, pdf.perfil).reindex(columns=ORDEM_PERFIS, fill_value=0)
tab_pct = tab.div(tab.sum(axis=1), axis=0) * 100

fig, axes = plt.subplots(1, 2, figsize=(17, 5.5))
for ax, dados, titulo, rotulo in [(axes[0], tab, "Quantidade de UBS por CRS e perfil", "UBS"),
                                  (axes[1], tab_pct, "Composição de cada CRS por perfil", "% das UBS da CRS")]:
    base = np.zeros(len(dados))
    for p in ORDEM_PERFIS:
        barras = ax.barh(dados.index, dados[p], left=base, color=CORES[p], label=p)
        if dados is tab_pct:
            ax.bar_label(barras, labels=[f"{v:.0f}%" if v >= 6 else "" for v in dados[p]],
                         label_type="center", fontsize=8)
        base += dados[p].values
    ax.invert_yaxis(); ax.set_title(titulo); ax.set_xlabel(rotulo)
axes[1].legend(loc="upper center", bbox_to_anchor=(-0.1, -0.12), ncol=4, fontsize=9)
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Gráficos: mapa das áreas de abrangência por perfil
import re
from matplotlib.patches import Patch

def aneis_externos(wkt_txt):
    """Extrai o anel externo de cada polígono de um (MULTI)POLYGON WKT, sem dependências externas."""
    corpo = re.sub(r"^\s*(MULTI)?POLYGON\s*(Z\s*)?", "", wkt_txt.strip(), flags=re.I)
    for poligono in re.split(r"\)\s*\)\s*,\s*\(\s*\(", corpo):
        anel = re.split(r"\)\s*,\s*\(", poligono)[0].strip("() ")
        pts = np.array([list(map(float, p.split()[:2])) for p in anel.split(",")])
        yield pts[:, 0], pts[:, 1]

areas = spark.sql("""
    SELECT cnes, geometria_wkt FROM workspace.default.ssp_silver_areas_abrangencia
    WHERE geometria_wkt IS NOT NULL
""").toPandas().merge(pdf[["cnes", "perfil"]], on="cnes", how="inner")
print(f"Áreas com geometria e perfil: {len(areas)} de {len(pdf)} UBS")

fig, ax = plt.subplots(figsize=(10, 14))
for _, r in areas.iterrows():
    for x, y in aneis_externos(r.geometria_wkt):
        ax.fill(x, y, facecolor=CORES[r.perfil], edgecolor="white", linewidth=0.2)
ax.scatter(pdf.longitude, pdf.latitude, s=3, c="black", alpha=0.6, label="UBS")
ax.set_aspect("equal"); ax.axis("off")
ax.legend(handles=[Patch(color=CORES[p], label=p) for p in ORDEM_PERFIS], loc="lower right", fontsize=9)
ax.set_title("Áreas de abrangência das UBS por perfil territorial", fontsize=14)
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Registro no MLflow
signature = infer_signature(X.head(100), model.predict(X.head(100)))

with mlflow.start_run(run_name="kmeans_ubs_perfis_k4") as run:
    mlflow.set_tags({"projeto": "saude_perto_sp", "tarefa": "clusterizacao"})
    mlflow.log_params({"k": K, "n_init": 10, "random_state": SEED,
                       "features": ",".join(FEATS), "features_log1p": ",".join(FEATS_LOG),
                       "imputacao_fcu": "constante 0", "n_linhas": len(X)})
    mlflow.log_metrics({"silhouette": sil, "davies_bouldin": db,
                        "ari_seed7": ari_runs[0], "ari_seed123": ari_runs[1]})
    mlflow.log_table(res_k, "selecao_k.json")
    mlflow.log_table(tabela_perfil, "perfil_clusters.json")
    model_info = mlflow.sklearn.log_model(model, name="model", signature=signature,
                                          input_example=X.head(3))

print("Run:", run.info.run_id)
print("Model URI:", model_info.model_uri)

# COMMAND ----------

# DBTITLE 1,Gravar perfis na Gold
TABELA_PERFIS = "workspace.default.ssp_gold_ubs_perfis"

# Distância de cada UBS ao centróide do próprio cluster (espaço padronizado): quanto menor, mais típica do perfil
dist_centroide = np.linalg.norm(Zf - model.named_steps["kmeans"].cluster_centers_[labels], axis=1)

saida = pdf[["cnes", "nome_ubs", "crs", "sts", "cluster", "perfil"] + FEATS].copy()
saida["cluster"] = saida["cluster"].astype("int32")
saida["distancia_centroide"] = dist_centroide
saida["mlflow_run_id"] = run.info.run_id
saida["model_uri"] = model_info.model_uri

spark.createDataFrame(saida).createOrReplaceTempView("vw_perfis_ubs")

# Reconstrução completa a cada execução (tabela derivada do modelo)
spark.sql(f"""
CREATE OR REPLACE TABLE {TABELA_PERFIS} (
  cnes STRING NOT NULL COMMENT 'CNES normalizado (7 dígitos) - chave lógica (ssp_gold_ubs_indicadores.cnes)',
  nome_ubs STRING COMMENT 'Nome de divulgação da UBS (descritivo; não é chave)',
  crs STRING COMMENT 'Coordenadoria Regional de Saúde',
  sts STRING COMMENT 'Supervisão Técnica de Saúde',
  cluster INT COMMENT 'Id do cluster KMeans (0 a 3); o significado está em perfil',
  perfil STRING COMMENT 'Nome do perfil territorial atribuído pelas medianas do cluster',
  pct_60_anos_ou_mais DOUBLE COMMENT 'Feature do modelo (Gold)',
  pct_0_a_4_anos DOUBLE COMMENT 'Feature do modelo (Gold)',
  taxa_analfabetismo_15_anos_ou_mais DOUBLE COMMENT 'Feature do modelo (Gold)',
  pct_pretos_pardos DOUBLE COMMENT 'Feature do modelo (Gold)',
  pct_unidades_domesticas_unipessoais DOUBLE COMMENT 'Feature do modelo (Gold)',
  pct_pessoas_fcu DOUBLE COMMENT 'Feature do modelo (Gold P_V001AA_FCU); NULL = sem registro de FCU, tratado como 0 no modelo',
  pct_domicilios_esgoto_nao_rede_geral DOUBLE COMMENT 'Feature do modelo (Gold; log1p no modelo)',
  densidade_demografica_hab_km2 DOUBLE COMMENT 'Feature do modelo (Gold; log1p no modelo)',
  renda_media_responsavel DOUBLE COMMENT 'Feature do modelo (Gold MEAN_V06004AAUBS2; log1p no modelo)',
  distancia_centroide DOUBLE COMMENT 'Distância euclidiana ao centróide do cluster no espaço padronizado (menor = mais típica)',
  mlflow_run_id STRING COMMENT 'Run MLflow que gerou a atribuição',
  model_uri STRING COMMENT 'URI do modelo MLflow (Pipeline pré-processamento + KMeans)',
  data_processamento TIMESTAMP COMMENT 'Data/hora da gravação (UTC)'
)
COMMENT 'Projeto Saúde Perto SP - Gold: perfil territorial de cada UBS (KMeans k=4 sobre ssp_gold_ubs_indicadores). 1 linha por CNES. Chave lógica: cnes.'
""")

# RTAS não aceita schema explícito: cria a estrutura (com comentários) e depois carrega
spark.sql(f"""
INSERT INTO {TABELA_PERFIS}
   SELECT cnes, nome_ubs, crs, sts, cluster, perfil,
          pct_60_anos_ou_mais, pct_0_a_4_anos, taxa_analfabetismo_15_anos_ou_mais, pct_pretos_pardos,
          pct_unidades_domesticas_unipessoais, pct_pessoas_fcu, pct_domicilios_esgoto_nao_rede_geral,
          densidade_demografica_hab_km2, renda_media_responsavel,
          distancia_centroide, mlflow_run_id, model_uri, current_timestamp() AS data_processamento
   FROM vw_perfis_ubs
""")

# Validação: 1 linha por CNES e mesmos tamanhos de cluster do modelo
display(spark.sql(f"""
  SELECT count(*) AS linhas, count(DISTINCT cnes) AS cnes_distintos,
         count_if(perfil IS NULL) AS perfis_nulos
  FROM {TABELA_PERFIS}"""))
display(spark.sql(f"SELECT perfil, count(*) AS qtd_ubs FROM {TABELA_PERFIS} GROUP BY perfil ORDER BY qtd_ubs DESC"))

# COMMAND ----------

# DBTITLE 1,View para BI
# MAGIC %sql
# MAGIC -- View de consumo para dashboards de BI (Databricks AI/BI, Power BI, Tableau via SQL Warehouse)
# MAGIC CREATE OR REPLACE VIEW workspace.default.vw_ssp_ubs_perfis_bi
# MAGIC COMMENT 'Projeto Saúde Perto SP - view para BI: perfil territorial de cada UBS (ssp_gold_ubs_perfis) com localização, população e polígono da área de abrangência. 1 linha por CNES.'
# MAGIC AS
# MAGIC SELECT p.cnes, p.nome_ubs, p.crs, p.sts, g.bairro, g.latitude, g.longitude,
# MAGIC        p.cluster, p.perfil, p.distancia_centroide,
# MAGIC        g.V001AA AS populacao_total, g.area_km2,
# MAGIC        p.pct_60_anos_ou_mais, p.pct_0_a_4_anos, p.taxa_analfabetismo_15_anos_ou_mais, p.pct_pretos_pardos,
# MAGIC        p.pct_unidades_domesticas_unipessoais, p.pct_pessoas_fcu, p.pct_domicilios_esgoto_nao_rede_geral,
# MAGIC        p.densidade_demografica_hab_km2, p.renda_media_responsavel,
# MAGIC        a.geometria_wkt AS area_abrangencia_wkt,
# MAGIC        p.mlflow_run_id, p.data_processamento
# MAGIC FROM workspace.default.ssp_gold_ubs_perfis p
# MAGIC JOIN workspace.default.ssp_gold_ubs_indicadores g ON g.cnes = p.cnes
# MAGIC LEFT JOIN workspace.default.ssp_silver_areas_abrangencia a ON a.cnes = p.cnes

# COMMAND ----------

# DBTITLE 1,Validar artefato em ambiente isolado
import shutil, subprocess, sys

# uv é necessário para recriar o ambiente do modelo do zero (mesmo comportamento do container de serving)
if shutil.which("uv") is None:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "uv"])

MODEL_URI = model_info.model_uri

pyfunc_model = mlflow.pyfunc.load_model(MODEL_URI)
if pyfunc_model.metadata.signature is None:
    raise ValueError("Modelo sem signature; corrigir o artefato antes do registro.")
exemplo = pyfunc_model.input_example
if exemplo is None:
    raise ValueError("Modelo sem input example; corrigir o artefato antes do registro.")

# Instala os pip_requirements do artefato do zero e executa predict
mlflow.models.predict(model_uri=MODEL_URI, input_data=exemplo, env_manager="uv")
print("Validação isolada OK:", MODEL_URI)

# COMMAND ----------

# DBTITLE 1,Registrar modelo no Unity Catalog
NOME_MODELO_UC = "workspace.default.ssp_modelo_perfis_ubs"

mlflow.set_registry_uri("databricks-uc")
registered_version = mlflow.register_model(
    model_uri=MODEL_URI,
    name=NOME_MODELO_UC,
    await_registration_for=300,
)
model_version = registered_version.version

from mlflow import MlflowClient
client = MlflowClient(registry_uri="databricks-uc")
client.update_registered_model(
    name=NOME_MODELO_UC,
    description=("Projeto Saúde Perto SP - KMeans (k=4) que atribui perfil territorial às UBS "
                 "a partir de 9 indicadores de ssp_gold_ubs_indicadores. Pipeline inclui imputação, "
                 "log1p e padronização; predict() retorna o id do cluster (0-3). Mapeamento "
                 "cluster -> perfil em workspace.default.ssp_gold_ubs_perfis."),
)

confirmado = client.get_model_version(name=NOME_MODELO_UC, version=str(model_version))
print(f"Modelo: {NOME_MODELO_UC} | versão: {confirmado.version} | status: {confirmado.status}")
print(f"URI: models:/{NOME_MODELO_UC}/{confirmado.version}")

# COMMAND ----------

# DBTITLE 1,Conclusões
# MAGIC %md
# MAGIC ## Conclusões
# MAGIC * **Qualidade:** silhouette 0,291, Davies-Bouldin 1,099. A estrutura é estável: o ARI contra as seeds 7 e 123 é de 0,967.
# MAGIC * **Perfis (medianas):**
# MAGIC   * **Urbano intermediário** (212 UBS): o perfil típico da cidade, com renda de cerca de R$ 1,9 mil e 11% da população em FCU.
# MAGIC   * **Urbano vulnerável** (143 UBS): 35% da população em FCU, 62% de pretos e pardos, renda de cerca de R$ 1,4 mil.
# MAGIC   * **Consolidado de alta renda** (111 UBS): renda de cerca de R$ 4,6 mil, 23% de idosos, analfabetismo de 1% e esgoto praticamente universal.
# MAGIC   * **Periferia de baixa densidade e sem saneamento** (19 UBS): 73% dos domicílios fora da rede geral de esgoto, cerca de 400 hab/km², o maior analfabetismo (6,7%).
# MAGIC * **Uso sugerido:** priorizar ações (saneamento, saúde da família, atenção ao idoso) conforme o perfil, e não apenas pela divisão administrativa CRS/STS.