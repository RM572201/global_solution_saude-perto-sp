# Databricks notebook source
# DBTITLE 1,1. Descrição e objetivo
# MAGIC %md
# MAGIC # 05 — Gold: indicadores por UBS | Projeto Saúde Perto SP
# MAGIC
# MAGIC **Objetivo:** gerar a tabela `workspace.default.ssp_gold_ubs_indicadores`, com **uma linha por UBS (CNES)**, integrando as três tabelas Silver:
# MAGIC * `workspace.default.ssp_silver_ubs_cadastro`
# MAGIC * `workspace.default.ssp_silver_areas_abrangencia`
# MAGIC * `workspace.default.ssp_silver_censo`
# MAGIC
# MAGIC **Conteúdo da Gold (52 colunas):** 12 atributos cadastrais e territoriais, `tem_dados_fcu`, 31 indicadores originais do Censo 2022 (30 aprovados + `V00585AAUBS`) e 8 indicadores derivados.
# MAGIC
# MAGIC **Origem do código:** as células de código deste notebook reproduzem, sem alteração de fórmulas, nomes de colunas, joins ou regras de negócio, o código efetivamente executado e validado em 17/11/2026 (gravação da versão Delta 1: 485 linhas, 414 UBS com FCU, 71 sem FCU, 13 validações pós-gravação aprovadas). Alterações em relação à execução original, somente organizacionais:
# MAGIC * a constante `TGT` e a chave `EXECUTAR_GRAVACAO` foram adicionadas à seção 2;
# MAGIC * a gravação (seção 8) foi isolada no final e só executa com `EXECUTAR_GRAVACAO = True`.
# MAGIC
# MAGIC **Regras aplicadas:** CNES como única chave; FCU ausente = NULL (nunca zero); denominador zero = NULL; percentuais na escala 0–100; sem arredondamento; nenhuma variável sem descrição no dicionário.
# MAGIC
# MAGIC **Validação do critério V5:** comparação dos indicadores derivados com tolerância absoluta de 1e-9 ponto percentual (aprovada pelo responsável do projeto; a diferença observada, 3,3e-14, é arredondamento de ponto flutuante).
# MAGIC
# MAGIC **Referência de reprodutibilidade:** o SHA-256 (16 primeiros caracteres) do SQL gerado na execução validada é `970862ececf49c8f` (impresso na seção 5).

# COMMAND ----------

# DBTITLE 1,2. Configuração e parâmetros
import hashlib
S="workspace.default.ssp_silver_censo"; C="workspace.default.ssp_silver_ubs_cadastro"; A="workspace.default.ssp_silver_areas_abrangencia"
TGT = "workspace.default.ssp_gold_ubs_indicadores"   # tabela Gold (adicionado aqui para uso nas seções 7 e 8)
EXECUTAR_GRAVACAO = False   # segurança: a seção 8 (CREATE TABLE / INSERT OVERWRITE) só grava com True

r=lambda a,b,suf="AAUBS": [f"V0{n}{suf}" for n in range(a,b+1)]
saida = [("basico",v) for v in ["V001AA","V002AA","V007AA"]] \
 + [("demografia",v) for v in ["V01007AAUBS","V01008AAUBS","V01031AAUBS","V01032AAUBS","V01033AAUBS","V01040AAUBS","V01041AAUBS"]] \
 + [("alfabetizacao",v) for v in ["V00900AAUBS","V00901AAUBS"]] \
 + [("racacor",v) for v in r(1317,1321)] \
 + [("rendamediaresp",v) for v in ["MEAN_V06004AAUBS2","V06001AAUBS"]] \
 + [("parentesco",v) for v in ["V01209AAUBS","V01194AAUBS"]] \
 + [("domicilios2",v) for v in ["V00111AAUBS","V00309AAUBS","V00397AAUBS","V00495AAUBS"]] \
 + [("domicilios1","V00050AAUBS")] \
 + [("FavelasComunidadesUrbanas",v) for v in ["V001AA_FCU","P_V001AA_FCU","V007AA_FCU","P_V007AA_FCU"]] \
 + [("domicilios3","V00585AAUBS")]
aux = [("demografia",v) for v in r(1031,1041)] + [("parentesco",v) for v in r(1209,1212)] \
 + [("domicilios2",v) for v in [f"V00{n}AAUBS" for n in range(309,317)]] + [("domicilios3",v) for v in [f"V00{n}AAUBS" for n in range(580,588)]]
todas = list(dict.fromkeys(saida+aux))
pares = ",".join(f"('{a}','{v}')" for a,v in todas)

# COMMAND ----------

# DBTITLE 1,3. Leitura das tabelas Silver (variáveis, descrições e status)
meta = spark.sql(f"""SELECT aba_origem, variavel_origem, max(descricao_variavel) d, collect_set(status_dicionario) sd,
  collect_set(status_valor) sv, count(*) n, count(DISTINCT cnes) nc, count(valor) nv
  FROM {S} WHERE (aba_origem, variavel_origem) IN ({pares}) GROUP BY ALL""").toPandas()
desc = {(x.aba_origem,x.variavel_origem): x.d for x in meta.itertuples()}

# COMMAND ----------

# DBTITLE 1,4. Integração por CNES (pivot do Censo e denominadores)
alias = lambda a,v: f"{a}__{v}"
piv = ",\n  ".join(f"max(CASE WHEN aba_origem='{a}' AND variavel_origem='{v}' THEN valor END) AS `{alias(a,v)}`" for a,v in todas)
q = lambda a,v: f"p.`{alias(a,v)}`"
soma = lambda lst: " + ".join(q(a,v) for a,v in lst)
den_idade = soma([("demografia",v) for v in r(1031,1041)])
den_cor   = soma([("racacor",v) for v in r(1317,1321)])
den_esg   = soma([("domicilios2",f"V00{n}AAUBS") for n in range(309,317)])
den_ud    = soma([("parentesco",v) for v in r(1209,1212)])
den_esg_p = soma([("domicilios3",f"V00{n}AAUBS") for n in range(580,588)])

# COMMAND ----------

# DBTITLE 1,5. Cálculo dos indicadores derivados e montagem da Gold (LEFT JOIN por CNES)
derivados = {
 "pct_60_anos_ou_mais": (f"100 * ({q('demografia','V01040AAUBS')} + {q('demografia','V01041AAUBS')}) / NULLIF({den_idade}, 0)",
     "Derivado: 100 * (V01040 + V01041) / soma(V01031..V01041), aba demografia"),
 "pct_0_a_4_anos": (f"100 * {q('demografia','V01031AAUBS')} / NULLIF({den_idade}, 0)",
     "Derivado: 100 * V01031 / soma(V01031..V01041), aba demografia"),
 "taxa_analfabetismo_15_anos_ou_mais": (f"100 * {q('alfabetizacao','V00901AAUBS')} / NULLIF({q('alfabetizacao','V00900AAUBS')} + {q('alfabetizacao','V00901AAUBS')}, 0)",
     "Derivado: 100 * V00901 / (V00900 + V00901), aba alfabetizacao"),
 "pct_pretos_pardos": (f"100 * ({q('racacor','V01318AAUBS')} + {q('racacor','V01320AAUBS')}) / NULLIF({den_cor}, 0)",
     "Derivado: 100 * (V01318 + V01320) / soma(V01317..V01321), aba racacor"),
 "pct_domicilios_esgoto_nao_rede_geral": (f"100 * (1 - {q('domicilios2','V00309AAUBS')} / NULLIF({den_esg}, 0))",
     "Derivado: 100 * (1 - V00309 / soma(V00309..V00316)), aba domicilios2; V00310 (fossa ligada à rede) conta como diferente de rede geral ou pluvial"),
 "pct_unidades_domesticas_unipessoais": (f"100 * {q('parentesco','V01209AAUBS')} / NULLIF({den_ud}, 0)",
     "Derivado: 100 * V01209 / soma(V01209..V01212), aba parentesco"),
 "densidade_demografica_hab_km2": (f"{q('basico','V001AA')} / NULLIF(a.area_km2, 0)",
     "Derivado: V001AA (basico) / area_km2 (ssp_silver_areas_abrangencia), pessoas por km²"),
 "pct_moradores_esgoto_rio_lago_corrego_mar": (f"100 * {q('domicilios3','V00585AAUBS')} / NULLIF({den_esg_p}, 0)",
     "Derivado: 100 * V00585 / soma(V00580..V00587), aba domicilios3"),
}
cols_orig = ",\n  ".join(f"{q(a,v)} AS `{v}`" for a,v in saida)
cols_der  = ",\n  ".join(f"{e} AS `{n}`" for n,(e,_) in derivados.items())
sql_gold = f"""
WITH p AS (
  SELECT cnes,
  {piv},
  count_if(aba_origem='FavelasComunidadesUrbanas') AS qtd_linhas_fcu
  FROM {S} WHERE (aba_origem, variavel_origem) IN ({pares}) OR aba_origem='FavelasComunidadesUrbanas'
  GROUP BY cnes)
SELECT c.cnes, c.nome_divulgacao AS nome_ubs, c.logradouro, c.numero_logradouro, c.complemento, c.bairro, c.cep,
  c.geom_latitude AS latitude, c.geom_longitude AS longitude,
  c.supervisao_tecnica_saude AS sts, c.coordenadoria_regional_saude AS crs, a.area_km2,
  coalesce(p.qtd_linhas_fcu, 0) > 0 AS tem_dados_fcu,
  {cols_orig},
  {cols_der}
FROM {C} c
LEFT JOIN {A} a ON a.cnes = c.cnes
LEFT JOIN p ON p.cnes = c.cnes"""
gold = spark.sql(sql_gold)
print(len(saida), len(todas), len(meta), len(gold.columns), hashlib.sha256(sql_gold.encode()).hexdigest()[:16])
print(set(sum(map(list,meta.sd),[])), all(desc[k] for k in saida))

# COMMAND ----------

# DBTITLE 1,6. Validações de qualidade (antes da gravação)
# MAGIC %md
# MAGIC ## 6. Validações de qualidade (antes da gravação)
# MAGIC
# MAGIC Somente leitura. Duas células, na ordem em que foram executadas:
# MAGIC 1. **V1 a V11** sobre o DataFrame `gold` (em memória), com recálculo independente a partir do formato longo da Silver.
# MAGIC 2. **V5 com tolerância absoluta de 1e-9 ponto percentual**, critério aprovado. Na primeira célula, a V5 usa tolerância **relativa** de 1e-12 e reporta `REPROVADA` para `pct_domicilios_esgoto_nao_rede_geral` (UBS 2027380, valor ≈ 2,55e-5, diferença absoluta 3,3e-14): falso negativo do critério relativo para valores próximos de zero, mantido aqui para rastreabilidade. O resultado válido da V5 é o da segunda célula.
# MAGIC
# MAGIC Se qualquer outra validação crítica for reprovada, não grave.

# COMMAND ----------

# DBTITLE 1,6.1 Validações V1 a V11
import numpy as np, pandas as pd
g = gold.toPandas()
res = []
def chk(nome, ok, det, critica=True): res.append((nome, "APROVADA" if ok else "REPROVADA", "crítica" if critica else "alerta", det))
cad = spark.table(C).select("cnes","nome_divulgacao","geom_latitude","geom_longitude").toPandas()
are = spark.table(A).select("cnes","area_km2").toPandas()
chk("V1 Unicidade de CNES", g.cnes.is_unique, f"{g.cnes.nunique()} distintos em {len(g)} linhas")
chk("V2 Exatamente 485 UBS", len(g)==485, f"{len(g)} linhas")
chk("V3 Correspondência com cadastro e áreas", set(g.cnes)==set(cad.cnes)==set(are.cnes) and g.area_km2.notna().all(),
    f"gold={len(set(g.cnes))}, cadastro={len(set(cad.cnes))}, áreas={len(set(are.cnes))}, iguais={set(g.cnes)==set(cad.cnes)==set(are.cnes)}, area_km2 nula={g.area_km2.isna().sum()}")
chk("V4 Sem multiplicação por joins", len(g)==len(cad) and cad.cnes.is_unique and are.cnes.is_unique and (meta.n==meta.nc).all(),
    f"gold={len(g)} = cadastro={len(cad)}; áreas únicas={are.cnes.is_unique}; censo 1 linha por CNES em todas as 54 variáveis={(meta.n==meta.nc).all()}")
# Recalculo independente a partir do formato longo
lg = spark.sql(f"SELECT cnes, aba_origem, variavel_origem, valor FROM {S} WHERE (aba_origem, variavel_origem) IN ({pares})").toPandas()
lg["k"]=lg.aba_origem+"|"+lg.variavel_origem
def ssum(a, vs): return lg[(lg.aba_origem==a)&lg.variavel_origem.isin(vs)].groupby("cnes").valor.sum()
def one(a, v): return lg[(lg.aba_origem==a)&(lg.variavel_origem==v)].set_index("cnes").valor
gi = g.set_index("cnes"); are_i = are.set_index("cnes").area_km2
esp = {
 "pct_60_anos_ou_mais": (ssum("demografia",["V01040AAUBS","V01041AAUBS"]), ssum("demografia",r(1031,1041))),
 "pct_0_a_4_anos": (one("demografia","V01031AAUBS"), ssum("demografia",r(1031,1041))),
 "taxa_analfabetismo_15_anos_ou_mais": (one("alfabetizacao","V00901AAUBS"), ssum("alfabetizacao",["V00900AAUBS","V00901AAUBS"])),
 "pct_pretos_pardos": (ssum("racacor",["V01318AAUBS","V01320AAUBS"]), ssum("racacor",r(1317,1321))),
 "pct_domicilios_esgoto_nao_rede_geral": (ssum("domicilios2",[f"V00{n}AAUBS" for n in range(310,317)]), ssum("domicilios2",[f"V00{n}AAUBS" for n in range(309,317)])),
 "pct_unidades_domesticas_unipessoais": (one("parentesco","V01209AAUBS"), ssum("parentesco",r(1209,1212))),
 "pct_moradores_esgoto_rio_lago_corrego_mar": (one("domicilios3","V00585AAUBS"), ssum("domicilios3",[f"V00{n}AAUBS" for n in range(580,588)])),
}
linhas=[]; ok5=True
for nome,(num,den) in esp.items():
    num=num.reindex(gi.index); den=den.reindex(gi.index)
    ref = 100*num/den.replace(0,np.nan)
    rel = ((gi[nome]-ref).abs()/ref.abs().clip(lower=1e-12)).max()
    ok = (rel<1e-12) and (num<=den+1e-9).all() and den.notna().all()
    ok5 &= ok
    linhas.append((nome, int((den==0).sum()), int(den.isna().sum()), int((num>den+1e-9).sum()), float(rel), float(gi[nome].min()), float(gi[nome].max())))
dens_ref = one("basico","V001AA").reindex(gi.index)/are_i.reindex(gi.index)
rel_d = ((gi.densidade_demografica_hab_km2-dens_ref).abs()/dens_ref).max()
linhas.append(("densidade_demografica_hab_km2", int((are_i==0).sum()), int(are_i.isna().sum()), 0, float(rel_d), float(gi.densidade_demografica_hab_km2.min()), float(gi.densidade_demografica_hab_km2.max())))
ok5 &= rel_d<1e-12
val = pd.DataFrame(linhas, columns=["indicador","den_zero","den_nulo","num>den","erro_rel_max_vs_recalculo","min","max"])
print(val.to_string(index=False))
chk("V5 Numeradores e denominadores", ok5, "recalculo independente (formato longo, pandas) coincide; nenhum denominador zero ou nulo; numerador ≤ denominador em todos")
pcts = [c for c in g.columns if c.startswith(("pct_","taxa_","P_V"))]
fora = {c: int(((g[c]<0)|(g[c]>100)).sum()) for c in pcts}
chk("V6 Percentuais entre 0 e 100", sum(fora.values())==0, f"colunas verificadas={len(pcts)}; fora do intervalo={fora}")
fcu_cols=["V001AA_FCU","P_V001AA_FCU","V007AA_FCU","P_V007AA_FCU"]
sem = g[~g.tem_dados_fcu]; com = g[g.tem_dados_fcu]
fcu_cnes = set(lg[lg.aba_origem=="FavelasComunidadesUrbanas"].cnes)
chk("V7 FCU: 71 sem dados e NULL", len(sem)==71 and len(com)==414 and sem[fcu_cols].isna().all().all() and com[fcu_cols].notna().all().all() and set(com.cnes)==fcu_cnes and (g[fcu_cols]==0).sum().sum()==(lg[(lg.aba_origem=='FavelasComunidadesUrbanas')&(lg.valor==0)].shape[0]),
    f"tem_dados_fcu true={len(com)}, false={len(sem)}; FCU nulos nas 71={int(sem[fcu_cols].isna().sum().sum())}/{71*4}; FCU preenchidos nas 414={int(com[fcu_cols].notna().sum().sum())}/{414*4}; zeros na Gold = zeros na Silver")
v585 = one("domicilios3","V00585AAUBS").reindex(gi.index)
chk("V8 V00585 e seu percentual", (gi.V00585AAUBS==v585).all() and linhas[6][4]<1e-12,
    f"V00585 idêntico à Silver em {int((gi.V00585AAUBS==v585).sum())}/485; soma={gi.V00585AAUBS.sum():.1f}; % com erro relativo máx {linhas[6][4]:.1e}; UBS com valor > 0: {int((gi.V00585AAUBS>0).sum())}")
# precisao: igualdade exata dos originais
difs = {}
for a,v in saida:
    s = one(a,v).reindex(gi.index)
    difs[v] = int(((gi[v]!=s) & ~(gi[v].isna() & s.isna())).sum())
cad_i=cad.set_index("cnes")
geo_ok = (gi.latitude==cad_i.geom_latitude.reindex(gi.index)).all() and (gi.longitude==cad_i.geom_longitude.reindex(gi.index)).all() and (gi.area_km2==are_i.reindex(gi.index)).all()
frac = int(sum(((gi[v]%1)!=0).sum() for a,v in saida if v not in fcu_cols))
chk("V9 Precisão decimal preservada", sum(difs.values())==0 and geo_ok, f"31 indicadores originais iguais bit a bit à Silver (divergências={sum(difs.values())}); lat/lon/area idênticos={geo_ok}; valores fracionários preservados={frac}; tipo DOUBLE em todos")
chk("V10 Indicadores sem descrição excluídos", all(desc[k] for k in saida), "todas as 31 variáveis com status_dicionario OK", True)
chk("V11 V001AA não duplicado", ("FavelasComunidadesUrbanas","V001AA") not in saida and list(g.columns).count("V001AA")==1, "apenas V001AA da aba basico")
rv = pd.DataFrame(res, columns=["validacao","resultado","tipo","detalhe"])
pd.set_option("display.max_colwidth",300); pd.set_option("display.width",300)
print(rv.to_string(index=False))
print("Nulos por coluna (exceto FCU):", {c:int(n) for c,n in g.drop(columns=fcu_cols).isna().sum().items() if n})

# COMMAND ----------

# DBTITLE 1,6.2 V5 com tolerância absoluta de 1e-9 (critério aprovado)
abs_err = {}
for nome,(num,dn) in esp.items():
    num=num.reindex(gi.index); dn=dn.reindex(gi.index)
    abs_err[nome] = float((gi[nome]-100*num/dn).abs().max())
abs_err["densidade_demografica_hab_km2"] = float((gi.densidade_demografica_hab_km2-dens_ref).abs().max())
print({k: f"{v:.2e}" for k,v in abs_err.items()})
print("V5 (tolerância absoluta 1e-9):", "APROVADA" if max(abs_err.values())<1e-9 else "REPROVADA")
print("max densidade:", gi.densidade_demografica_hab_km2.idxmax(), gi.loc[gi.densidade_demografica_hab_km2.idxmax(), ["nome_ubs","V001AA","area_km2"]].to_dict())

# COMMAND ----------

# DBTITLE 1,7. Consulta da Gold persistida para conferência
# MAGIC %md
# MAGIC ## 7. Consulta da Gold persistida para conferência
# MAGIC
# MAGIC Somente leitura. Lê `workspace.default.ssp_gold_ubs_indicadores` e confere a tabela gravada contra as tabelas Silver (13 validações, todas aprovadas na versão Delta 1 de 17/11/2026). A última linha imprime a versão Delta atual.

# COMMAND ----------

# DBTITLE 1,7.1 Validações da tabela gravada (13)
import numpy as np, pandas as pd
t = spark.table(TGT).toPandas(); ti = t.set_index("cnes")
cad = spark.table(C).select("cnes","geom_latitude","geom_longitude").toPandas().set_index("cnes")
are_i = spark.table(A).select("cnes","area_km2").toPandas().set_index("cnes").area_km2
lg = spark.sql(f"SELECT cnes, aba_origem, variavel_origem, valor FROM {S} WHERE (aba_origem, variavel_origem) IN ({pares})").toPandas()
fcu_cnes = set(spark.sql(f"SELECT DISTINCT cnes FROM {S} WHERE aba_origem='FavelasComunidadesUrbanas'").toPandas().cnes)
def ssum(a, vs): return lg[(lg.aba_origem==a)&lg.variavel_origem.isin(vs)].groupby("cnes").valor.sum().reindex(ti.index)
def one(a, v): return lg[(lg.aba_origem==a)&(lg.variavel_origem==v)].set_index("cnes").valor.reindex(ti.index)
orig = [v for a,v in saida]; der = list(derivados); fcu_cols=["V001AA_FCU","P_V001AA_FCU","V007AA_FCU","P_V007AA_FCU"]
res=[]
def chk(n, ok, d): res.append((n, "APROVADA" if ok else "REPROVADA", d))
chk("Linhas = 485", len(t)==485, f"{len(t)}")
chk("CNES distintos = 485 e únicos", t.cnes.nunique()==485 and t.cnes.is_unique, f"{t.cnes.nunique()}")
chk("CNES = cadastro = áreas", set(t.cnes)==set(cad.index)==set(are_i.index), f"{len(set(t.cnes)&set(cad.index)&set(are_i.index))} em comum")
chk("UBS com FCU = 414", int(t.tem_dados_fcu.sum())==414 and set(t[t.tem_dados_fcu].cnes)==fcu_cnes, f"{int(t.tem_dados_fcu.sum())}; igual ao conjunto da Silver={set(t[t.tem_dados_fcu].cnes)==fcu_cnes}")
sem=t[~t.tem_dados_fcu]
chk("UBS sem FCU = 71 com FCU nulos", len(sem)==71 and int(sem[fcu_cols].isna().sum().sum())==284 and int(t[t.tem_dados_fcu][fcu_cols].isna().sum().sum())==0, f"{len(sem)} UBS; nulos {int(sem[fcu_cols].isna().sum().sum())}/284; nulos nas 414={int(t[t.tem_dados_fcu][fcu_cols].isna().sum().sum())}")
chk("Indicadores originais = 31", len(orig)==31 and all(c in t.columns for c in orig), f"{sum(c in t.columns for c in orig)} presentes")
chk("Indicadores derivados = 8", len(der)==8 and all(c in t.columns for c in der), f"{sum(c in t.columns for c in der)} presentes")
difs = sum(int(((ti[v]!=one(a,v)) & ~(ti[v].isna() & one(a,v).isna())).sum()) for a,v in saida)
chk("Originais idênticos à Silver (bit a bit)", difs==0, f"divergências={difs}")
geo = (ti.latitude==cad.geom_latitude.reindex(ti.index)).all() and (ti.longitude==cad.geom_longitude.reindex(ti.index)).all() and (ti.area_km2==are_i.reindex(ti.index)).all()
chk("Lat/lon/área idênticos às Silver", geo, str(geo))
esp = {
 "pct_60_anos_ou_mais": 100*ssum("demografia",["V01040AAUBS","V01041AAUBS"])/ssum("demografia",r(1031,1041)),
 "pct_0_a_4_anos": 100*one("demografia","V01031AAUBS")/ssum("demografia",r(1031,1041)),
 "taxa_analfabetismo_15_anos_ou_mais": 100*one("alfabetizacao","V00901AAUBS")/ssum("alfabetizacao",["V00900AAUBS","V00901AAUBS"]),
 "pct_pretos_pardos": 100*ssum("racacor",["V01318AAUBS","V01320AAUBS"])/ssum("racacor",r(1317,1321)),
 "pct_domicilios_esgoto_nao_rede_geral": 100*ssum("domicilios2",[f"V00{n}AAUBS" for n in range(310,317)])/ssum("domicilios2",[f"V00{n}AAUBS" for n in range(309,317)]),
 "pct_unidades_domesticas_unipessoais": 100*one("parentesco","V01209AAUBS")/ssum("parentesco",r(1209,1212)),
 "densidade_demografica_hab_km2": one("basico","V001AA")/are_i.reindex(ti.index),
 "pct_moradores_esgoto_rio_lago_corrego_mar": 100*one("domicilios3","V00585AAUBS")/ssum("domicilios3",[f"V00{n}AAUBS" for n in range(580,588)]),
}
err = {k: float((ti[k]-v).abs().max()) for k,v in esp.items()}
nulos_der = {k:int(ti[k].isna().sum()) for k in der}
chk("Derivados vs recálculo (tol. abs. 1e-9)", max(err.values())<1e-9 and sum(nulos_der.values())==0, f"erro abs máx={max(err.values()):.2e}; nulos={sum(nulos_der.values())}")
pcts=[c for c in t.columns if c.startswith(("pct_","taxa_","P_V"))]
fora=sum(int(((t[c]<0)|(t[c]>100)).sum()) for c in pcts)
chk("Percentuais em [0,100]", fora==0, f"{len(pcts)} colunas; fora={fora}")
chk("V00585 idêntico à Silver", (ti.V00585AAUBS==one("domicilios3","V00585AAUBS")).all(), f"soma={ti.V00585AAUBS.sum():.1f}")
desc_ok = spark.sql(f"DESCRIBE TABLE {TGT}").toPandas()
sem_coment = int((desc_ok.comment.isna() | (desc_ok.comment=="")).sum())
chk("Comentários em todas as colunas", sem_coment==0 and len(desc_ok)==52, f"{len(desc_ok)} colunas; sem comentário={sem_coment}")
pd.set_option("display.width",250); pd.set_option("display.max_colwidth",120)
print(pd.DataFrame(res, columns=["validacao","resultado","detalhe"]).to_string(index=False))
print({k:f"{v:.1e}" for k,v in err.items()})
print("versao atual:", spark.sql(f"DESCRIBE HISTORY {TGT} LIMIT 1").collect()[0]["version"])

# COMMAND ----------

# DBTITLE 1,8. Gravação (isolada e desabilitada)
# MAGIC %md
# MAGIC ## 8. Gravação da Gold (isolada e desabilitada)
# MAGIC
# MAGIC **Atenção:** estas células só gravam quando `EXECUTAR_GRAVACAO = True` (seção 2). Com o valor padrão `False`, `CREATE TABLE` e `INSERT OVERWRITE` não são executados; as demais linhas apenas leem metadados.
# MAGIC
# MAGIC Para regravar: (1) execute as seções 2 a 6 e confirme todas as validações; (2) altere `EXECUTAR_GRAVACAO` para `True`; (3) execute 8.1 e 8.2; (4) execute a seção 7.
# MAGIC
# MAGIC * **8.1** `CREATE TABLE IF NOT EXISTS` com o esquema validado (52 colunas, comentários; `cnes` e `tem_dados_fcu` NOT NULL). Na execução original gerou a versão Delta 0.
# MAGIC * **8.2** `INSERT OVERWRITE` com a consulta validada (`sql_gold`): substituição completa, sem append. Na execução original gerou a versão Delta 1 (485 linhas, 1 arquivo).
# MAGIC
# MAGIC Única diferença em relação ao código original: as chamadas `spark.sql(ddl)` e `spark.sql(INSERT OVERWRITE ...)` estão dentro de `if EXECUTAR_GRAVACAO:`.

# COMMAND ----------

# DBTITLE 1,8.1 CREATE TABLE IF NOT EXISTS (desabilitado por padrão)
esc = lambda s: s.replace("\\","\\\\").replace("'","\\'")
com = {
 "cnes":"CNES normalizado (texto, 7 dígitos) - chave de integração (ssp_silver_ubs_cadastro.cnes)",
 "nome_ubs":"Nome de divulgação da UBS (ssp_silver_ubs_cadastro.nome_divulgacao; descritivo, não é chave)",
 "logradouro":"Logradouro (ssp_silver_ubs_cadastro.logradouro)",
 "numero_logradouro":"Número do logradouro, texto (ssp_silver_ubs_cadastro.numero_logradouro)",
 "complemento":"Complemento do endereço (ssp_silver_ubs_cadastro.complemento)",
 "bairro":"Bairro (ssp_silver_ubs_cadastro.bairro)",
 "cep":"CEP como na origem (ssp_silver_ubs_cadastro.cep)",
 "latitude":"Latitude do ponto da UBS no KML, EPSG:4326 (ssp_silver_ubs_cadastro.geom_latitude)",
 "longitude":"Longitude do ponto da UBS no KML, EPSG:4326 (ssp_silver_ubs_cadastro.geom_longitude)",
 "sts":"Supervisão Técnica de Saúde (ssp_silver_ubs_cadastro.supervisao_tecnica_saude)",
 "crs":"Coordenadoria Regional de Saúde (ssp_silver_ubs_cadastro.coordenadoria_regional_saude)",
 "area_km2":"Área oficial de abrangência em km² (ssp_silver_areas_abrangencia.area_km2)",
 "tem_dados_fcu":"Verdadeiro se o CNES possui registros na aba FavelasComunidadesUrbanas; falso quando não há registros (indicadores FCU ficam NULL)",
}
for a,v in saida: com[v] = f"{desc[(a,v)]} [aba {a}, variável {v}]"
for n,(_,cm) in derivados.items(): com[n] = cm
assert list(com) == gold.columns, "ordem/nomes divergem do esquema validado"
tipos = {f.name: f.dataType.simpleString().upper() for f in gold.schema.fields}
TGT = "workspace.default.ssp_gold_ubs_indicadores"
cols = ",\n".join(f"  `{c}` {tipos[c]}{' NOT NULL' if c in ('cnes','tem_dados_fcu') else ''} COMMENT '{esc(com[c])}'" for c in gold.columns)
ddl = f"""CREATE TABLE IF NOT EXISTS {TGT} (
{cols}
) USING DELTA
COMMENT 'Projeto Saúde Perto SP - Gold: indicadores por UBS (1 linha por CNES) integrando cadastro, áreas de abrangência e Censo 2022 estimado. Chave lógica: cnes. Sem constraints físicas.'"""
if EXECUTAR_GRAVACAO:
    spark.sql(ddl)
else:
    print("Gravação desabilitada (EXECUTAR_GRAVACAO = False): CREATE TABLE não executado.")
sch = spark.table(TGT).schema
print("colunas:", len(sch), "| ordem igual:", [f.name for f in sch]==gold.columns,
      "| tipos iguais:", all(f.dataType==g.dataType for f,g in zip(sch, gold.schema.fields)),
      "| linhas atuais:", spark.table(TGT).count())
spark.sql(f"DESCRIBE HISTORY {TGT}").select("version","timestamp","operation").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,8.2 INSERT OVERWRITE (desabilitado por padrão)
if EXECUTAR_GRAVACAO:
    spark.sql(f"INSERT OVERWRITE {TGT} SELECT * FROM ({sql_gold})")
else:
    print("Gravação desabilitada (EXECUTAR_GRAVACAO = False): INSERT OVERWRITE não executado.")
h = spark.sql(f"DESCRIBE HISTORY {TGT}").select("version","timestamp","operation","operationParameters","operationMetrics").toPandas()
for x in h.itertuples(): print(x.version, x.timestamp, x.operation, dict(x.operationParameters or {}), {k:v for k,v in (x.operationMetrics or {}).items() if k in ('numOutputRows','numFiles','numOutputBytes')})