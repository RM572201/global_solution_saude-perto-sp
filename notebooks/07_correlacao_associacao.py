# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Introdução
# MAGIC %md
# MAGIC # Correlação e associação | Projeto Saúde Perto SP
# MAGIC
# MAGIC **Objetivo:** medir como os indicadores socioterritoriais das 485 UBS se relacionam entre si, com a divisão administrativa (CRS) e com os perfis da clusterização.
# MAGIC
# MAGIC **Fontes:** `workspace.default.ssp_gold_ubs_indicadores` e `workspace.default.ssp_gold_ubs_perfis`.
# MAGIC
# MAGIC **Técnicas:**
# MAGIC | Tipo de par | Medida | Teste |
# MAGIC | --- | --- | --- |
# MAGIC | Numérica × numérica | Pearson (linear) e Spearman (monotônica, robusta a caudas longas) | p-valor da correlação |
# MAGIC | Numérica × categórica (CRS, perfil) | Razão de correlação η² (parte da variância explicada pelo grupo) | Kruskal-Wallis |
# MAGIC | Categórica × categórica (perfil × CRS) | V de Cramér | Qui-quadrado |
# MAGIC
# MAGIC **Cuidados de interpretação:**
# MAGIC * Os dados cobrem **todas** as UBS (não é amostra). Os p-valores são mostrados como referência; o que importa é a **força** da relação.
# MAGIC * Correlação não é causalidade.
# MAGIC * Os dados são agregados por área (falácia ecológica): uma relação entre áreas não vale necessariamente para indivíduos.
# MAGIC * `pct_pessoas_fcu` é NULL em 71 áreas sem registro de FCU. Aqui esses casos ficam **fora** dos cálculos (exclusão par a par), e não são tratados como 0.

# COMMAND ----------

# DBTITLE 1,Imports e leitura dos dados
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from itertools import combinations
from scipy import stats

# Indicadores relativos (taxas, percentuais, densidade, renda média); contagens absolutas ficam de fora
INDICADORES = {
    "pct_60_anos_ou_mais": "% 60+ anos",
    "pct_0_a_4_anos": "% 0-4 anos",
    "taxa_analfabetismo_15_anos_ou_mais": "Analfabetismo 15+",
    "pct_pretos_pardos": "% pretos e pardos",
    "pct_unidades_domesticas_unipessoais": "% unipessoais",
    "pct_pessoas_fcu": "% pessoas em FCU",
    "pct_domicilios_esgoto_nao_rede_geral": "% esgoto fora rede",
    "pct_moradores_esgoto_rio_lago_corrego_mar": "% esgoto em rio/córrego",
    "densidade_demografica_hab_km2": "Densidade (hab/km²)",
    "renda_media_responsavel": "Renda média resp.",
}
VARS = list(INDICADORES)

pdf = spark.sql("""
    SELECT g.cnes, g.nome_ubs, g.crs,
           g.pct_60_anos_ou_mais, g.pct_0_a_4_anos, g.taxa_analfabetismo_15_anos_ou_mais,
           g.pct_pretos_pardos, g.pct_unidades_domesticas_unipessoais,
           g.P_V001AA_FCU AS pct_pessoas_fcu,
           g.pct_domicilios_esgoto_nao_rede_geral, g.pct_moradores_esgoto_rio_lago_corrego_mar,
           g.densidade_demografica_hab_km2,
           g.MEAN_V06004AAUBS2 AS renda_media_responsavel,
           p.perfil
    FROM workspace.default.ssp_gold_ubs_indicadores g
    LEFT JOIN workspace.default.ssp_gold_ubs_perfis p USING (cnes)
""").toPandas()

print("Shape:", pdf.shape, "| CNES distintos:", pdf.cnes.nunique(), "| sem perfil:", pdf.perfil.isna().sum())
print("Nulos:\n", pdf[VARS].isna().sum()[lambda s: s > 0])

# COMMAND ----------

# DBTITLE 1,Matrizes de correlação (Pearson e Spearman)
rotulos = [INDICADORES[v] for v in VARS]
pearson = pdf[VARS].corr(method="pearson")
spearman = pdf[VARS].corr(method="spearman")

fig, axes = plt.subplots(1, 2, figsize=(18, 7.5))
for ax, mat, titulo in zip(axes, [pearson, spearman], ["Pearson", "Spearman"]):
    im = ax.imshow(mat, cmap="RdBu_r", vmin=-1, vmax=1)
    ax.set_xticks(range(len(VARS)), rotulos, rotation=60, ha="right", fontsize=8)
    ax.set_yticks(range(len(VARS)), rotulos, fontsize=8)
    for i in range(len(VARS)):
        for j in range(len(VARS)):
            ax.text(j, i, f"{mat.iloc[i, j]:.2f}", ha="center", va="center", fontsize=7)
    ax.set_title(f"Correlação de {titulo}")
fig.colorbar(im, ax=axes, shrink=0.8)
plt.show()

# COMMAND ----------

# DBTITLE 1,Ranking dos pares de indicadores
def forca(r):
    a = abs(r)
    return "muito forte" if a >= 0.8 else "forte" if a >= 0.6 else "moderada" if a >= 0.4 else "fraca" if a >= 0.2 else "desprezível"

pares = []
for a, b in combinations(VARS, 2):
    d = pdf[[a, b]].dropna()  # exclusão par a par (FCU)
    r_p, p_p = stats.pearsonr(d[a], d[b])
    r_s, p_s = stats.spearmanr(d[a], d[b])
    pares.append({"variavel_1": a, "variavel_2": b, "n": len(d),
                  "pearson": r_p, "p_pearson": p_p, "spearman": r_s, "p_spearman": p_s,
                  "forca_spearman": forca(r_s)})
pares = pd.DataFrame(pares)
pares["abs_spearman"] = pares.spearman.abs()
pares = pares.sort_values("abs_spearman", ascending=False).reset_index(drop=True)

# Diferença grande entre Pearson e Spearman indica relação não linear ou efeito de outliers
pares["dif_pearson_spearman"] = (pares.pearson - pares.spearman).abs()

display(pares.drop(columns="abs_spearman").round(4).head(15))

# COMMAND ----------

# DBTITLE 1,Gráfico: ranking das correlações
top15 = pares.head(15).iloc[::-1]
nomes_pares = [f"{INDICADORES[a]} × {INDICADORES[b]}" for a, b in zip(top15.variavel_1, top15.variavel_2)]
y = np.arange(len(top15))

fig, ax = plt.subplots(figsize=(11, 7))
cores = ["#d7191c" if v < 0 else "#2b83ba" for v in top15.spearman]
ax.barh(y, top15.spearman, color=cores, height=0.6, label="Spearman")
ax.scatter(top15.pearson, y, color="black", marker="D", s=25, zorder=3, label="Pearson")
for yi, v in zip(y, top15.spearman):
    ax.text(v + (0.02 if v > 0 else -0.02), yi, f"{v:.2f}", va="center", ha="left" if v > 0 else "right", fontsize=8)
ax.axvline(0, color="grey", lw=0.8)
for lim in (-0.6, 0.6):
    ax.axvline(lim, color="grey", lw=0.6, ls="--")
ax.set_yticks(y, nomes_pares, fontsize=9); ax.set_xlim(-1.1, 1.1)
ax.set_xlabel("Correlação (azul = positiva, vermelho = negativa; tracejado = |r| 0,6)")
ax.set_title("15 pares de indicadores com maior correlação (Spearman)")
ax.legend(loc="lower right")
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Dispersão dos pares mais fortes
top = pares.head(6)
fig, axes = plt.subplots(2, 3, figsize=(16, 9))
for ax, (_, row) in zip(axes.ravel(), top.iterrows()):
    a, b = row.variavel_1, row.variavel_2
    d = pdf[[a, b]].dropna()
    ax.scatter(d[a], d[b], s=10, alpha=0.6)
    # escala log quando a variável tem cauda longa
    for eixo, v in (("x", a), ("y", b)):
        if v in ("densidade_demografica_hab_km2", "renda_media_responsavel"):
            getattr(ax, f"set_{eixo}scale")("log")
    ax.set_xlabel(INDICADORES[a]); ax.set_ylabel(INDICADORES[b])
    ax.set_title(f"Spearman {row.spearman:.2f} | Pearson {row.pearson:.2f}", fontsize=10)
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Associação dos indicadores com CRS e perfil (η² e Kruskal-Wallis)
def eta_quadrado(y, grupos):
    """Razão de correlação: variância entre grupos / variância total."""
    media = y.mean()
    ss_total = ((y - media) ** 2).sum()
    ss_entre = sum(len(g) * (g.mean() - media) ** 2 for _, g in y.groupby(grupos))
    return ss_entre / ss_total

assoc = []
for grupo in ["crs", "perfil"]:
    for v in VARS:
        d = pdf[[v, grupo]].dropna()
        amostras = [g[v].values for _, g in d.groupby(grupo)]
        h, p = stats.kruskal(*amostras)
        assoc.append({"grupo": grupo, "indicador": v, "n": len(d),
                      "eta2": eta_quadrado(d[v], d[grupo]), "kruskal_h": h, "p_kruskal": p})
assoc = pd.DataFrame(assoc)

tab_eta = assoc.pivot(index="indicador", columns="grupo", values="eta2").rename(index=INDICADORES)
tab_eta = tab_eta.sort_values("perfil", ascending=False)
display(tab_eta.round(3).reset_index())

ax = tab_eta.plot.barh(figsize=(9, 5))
ax.set_xlabel("η² (parte da variação explicada pelo grupo)"); ax.invert_yaxis()
ax.set_title("Quanto CRS e perfil explicam cada indicador")
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Medianas por CRS
medianas_crs = pdf.groupby("crs")[VARS].median().rename(columns=INDICADORES)
medianas_crs.insert(0, "qtd_ubs", pdf.groupby("crs").size())
display(medianas_crs.round(1).reset_index())

# COMMAND ----------

# DBTITLE 1,Gráficos: indicadores por CRS
ordem_crs = medianas_crs.index.tolist()

# 1) Mapa de calor: mediana de cada CRS comparada às demais (desvios-padrão entre as medianas das CRS)
med = medianas_crs.drop(columns="qtd_ubs")
z_crs = (med - med.mean()) / med.std()
fig, ax = plt.subplots(figsize=(13, 4.5))
im = ax.imshow(z_crs, cmap="RdBu_r", vmin=-2, vmax=2, aspect="auto")
ax.set_xticks(range(med.shape[1]), med.columns, rotation=35, ha="right")
ax.set_yticks(range(len(ordem_crs)), ordem_crs)
for i in range(med.shape[0]):
    for j in range(med.shape[1]):
        ax.text(j, i, f"{med.iloc[i, j]:,.1f}".replace(",", "X").replace(".", ",").replace("X", "."),
                ha="center", va="center", fontsize=8)
fig.colorbar(im, ax=ax, label="acima (vermelho) / abaixo (azul) das demais CRS")
ax.set_title("Mediana dos indicadores por CRS (valor na célula; cor = posição relativa)")
plt.tight_layout(); plt.show()

# 2) Boxplots: variação dentro de cada CRS (mostra por que o η² por CRS é baixo)
sel = ["taxa_analfabetismo_15_anos_ou_mais", "pct_pretos_pardos", "pct_pessoas_fcu",
       "pct_domicilios_esgoto_nao_rede_geral", "renda_media_responsavel", "pct_60_anos_ou_mais"]
eta_crs = assoc[assoc.grupo == "crs"].set_index("indicador").eta2
fig, axes = plt.subplots(2, 3, figsize=(17, 9))
for ax, v in zip(axes.ravel(), sel):
    ax.boxplot([pdf.loc[pdf.crs == c, v].dropna() for c in ordem_crs], patch_artist=True,
               boxprops={"facecolor": "#abd9e9"}, flierprops={"markersize": 3, "alpha": 0.5})
    if v == "renda_media_responsavel":
        ax.set_yscale("log")
    ax.set_xticks(range(1, len(ordem_crs) + 1), ordem_crs, fontsize=9)
    ax.set_title(f"{INDICADORES[v]}  (η² CRS = {eta_crs[v]:.2f})")
    ax.grid(axis="y", alpha=0.3)
fig.suptitle("Distribuição dos indicadores dentro de cada CRS", fontsize=14)
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Perfil × CRS (qui-quadrado e V de Cramér)
tab = pd.crosstab(pdf.perfil, pdf.crs)
chi2, p_chi2, gl, esperado = stats.chi2_contingency(tab)
n = tab.values.sum()
v_cramer = np.sqrt(chi2 / (n * (min(tab.shape) - 1)))

print(f"Qui-quadrado = {chi2:.1f} | gl = {gl} | p = {p_chi2:.2e} | V de Cramér = {v_cramer:.3f}")
print(f"Células com frequência esperada < 5: {(esperado < 5).sum()} de {esperado.size} "
      "(o p-valor do qui-quadrado fica aproximado; o V de Cramér continua válido como medida descritiva)")

# Distribuição dos perfis dentro de cada CRS (% da coluna)
display((tab / tab.sum() * 100).round(1).reset_index())

# COMMAND ----------

# DBTITLE 1,Gráfico: perfil × CRS (contagens e resíduos)
# Resíduo padronizado do qui-quadrado: (observado - esperado) / raiz(esperado)
# > +2: combinação mais frequente que o esperado ao acaso; < -2: menos frequente
residuo = (tab - esperado) / np.sqrt(esperado)

fig, ax = plt.subplots(figsize=(11, 4.5))
lim = np.abs(residuo.values).max()
im = ax.imshow(residuo, cmap="RdBu_r", vmin=-lim, vmax=lim, aspect="auto")
ax.set_xticks(range(tab.shape[1]), tab.columns)
ax.set_yticks(range(tab.shape[0]), tab.index)
for i in range(tab.shape[0]):
    for j in range(tab.shape[1]):
        ax.text(j, i, f"{tab.iloc[i, j]}\n({residuo.iloc[i, j]:+.1f})", ha="center", va="center", fontsize=8)
fig.colorbar(im, ax=ax, label="resíduo padronizado")
ax.set_title(f"Perfil × CRS: quantidade de UBS (resíduo)  |  V de Cramér = {v_cramer:.2f}")
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Gravar resultados para BI
TABELA = "workspace.default.ssp_gold_associacoes"

# Formato longo, único para os três tipos de análise
linhas = []
for _, r in pares.iterrows():
    linhas += [
        ("numerica_x_numerica", r.variavel_1, r.variavel_2, "pearson", r.pearson, r.p_pearson, r.n),
        ("numerica_x_numerica", r.variavel_1, r.variavel_2, "spearman", r.spearman, r.p_spearman, r.n),
    ]
for _, r in assoc.iterrows():
    linhas.append(("numerica_x_categorica", r.indicador, r.grupo, "eta2", r.eta2, r.p_kruskal, r.n))
linhas.append(("categorica_x_categorica", "perfil", "crs", "v_cramer", v_cramer, p_chi2, n))

resultado = pd.DataFrame(linhas, columns=["tipo_analise", "variavel_1", "variavel_2", "medida", "valor", "p_valor", "n"])
resultado["rotulo_1"] = resultado.variavel_1.map(INDICADORES).fillna(resultado.variavel_1)
resultado["rotulo_2"] = resultado.variavel_2.map(INDICADORES).fillna(resultado.variavel_2)
resultado["n"] = resultado["n"].astype("int64")

(spark.createDataFrame(resultado)
      .selectExpr("*", "current_timestamp() AS data_processamento")
      .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABELA))

spark.sql(f"""COMMENT ON TABLE {TABELA} IS 'Projeto Saúde Perto SP - Gold: medidas de correlação e associação entre indicadores por UBS, CRS e perfil (notebook 07). Formato longo: 1 linha por tipo_analise + variavel_1 + variavel_2 + medida.'""")

display(spark.sql(f"SELECT tipo_analise, medida, count(*) AS linhas FROM {TABELA} GROUP BY ALL ORDER BY ALL"))
print("Esperado:", len(pares), "pares x 2 medidas,", len(assoc), "linhas eta2, 1 linha V de Cramér")

# COMMAND ----------

# DBTITLE 1,Conclusões
# MAGIC %md
# MAGIC ## Conclusões
# MAGIC * **Eixo de vulnerabilidade:** analfabetismo, % de pretos e pardos, % de crianças de 0 a 4 anos e baixa renda andam juntos (Spearman entre 0,76 e 0,89 em valor absoluto). Por exemplo, analfabetismo × % de pretos e pardos dá 0,89 e analfabetismo × renda dá -0,84.
# MAGIC * **Eixo etário:** % de idosos × % de crianças dá -0,94. Áreas mais envelhecidas também têm menos analfabetismo (-0,83) e menos pretos e pardos (-0,84).
# MAGIC * **Favelas e comunidades:** % de pessoas em FCU × analfabetismo dá 0,75, nas 414 áreas com registro.
# MAGIC * **Pearson × Spearman:** a diferença é grande em pares com esgoto e renda (a maior é 0,43, em % pretos e pardos × % esgoto fora da rede). Isso indica relação não linear e efeito de valores extremos, por isso o ranking usa Spearman.
# MAGIC * **CRS explica pouco:** o η² por CRS chega a no máximo 0,47 (% unipessoais) e fica abaixo de 0,12 para saneamento e FCU. A desigualdade está **dentro** das coordenadorias.
# MAGIC * **Perfis explicam mais:** o η² por perfil chega a 0,75 (esgoto) e 0,74 (% pretos e pardos). Isso é esperado, porque os perfis foram criados a partir desses mesmos indicadores; não é uma validação independente.
# MAGIC * **Perfil × CRS:** o V de Cramér é 0,379 (associação moderada). O p-valor do qui-quadrado é aproximado, porque 8 das 24 células têm frequência esperada abaixo de 5.