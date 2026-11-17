# Databricks notebook source
# DBTITLE 1,Apresentação do notebook
# MAGIC %md
# MAGIC # 03 — Silver: áreas de abrangência das UBS | Projeto Saúde Perto SP
# MAGIC
# MAGIC **Objetivo:** estruturar as 485 áreas de abrangência a partir da fonte oficial `Areas_de_Abrangencia_de_UBS_no_MSP_2026.kmz`, lida da cópia Bronze no Volume. Sem camada Gold, indicadores ou dashboard.
# MAGIC
# MAGIC **Entrada:** `doc.kml` dentro do KMZ, com 485 *placemarks* `MultiGeometry` (1 ou mais polígonos, com ou sem furos) e atributos em tabela HTML: `CNES_UBS`, `NOME_UBS`, `STS_UBS`, `CRS_UBS`, `AREAKM2`.
# MAGIC
# MAGIC **Saída:** `workspace.default.ssp_silver_areas_abrangencia`, uma linha por CNES. As validações são exibidas no notebook, e as inconsistências por registro ficam em colunas de status. Não há tabela de qualidade separada.
# MAGIC
# MAGIC **Regras de tratamento**
# MAGIC 1. **Leitura:** o KMZ é aberto como ZIP somente para leitura. O `doc.kml` usa o prefixo `xsi` sem declará-lo; a declaração é incluída **apenas em memória** para o XML ser lido. O arquivo não é alterado.
# MAGIC 2. **Atributos:** extraídos dos pares `<td>CHAVE</td><td>valor</td>`; texto vazio vira `NULL`; nada é inferido. O HTML original é preservado.
# MAGIC 3. **CNES:** mesma regra do notebook 02 (texto de 7 dígitos; inválido bloqueia a gravação). É a única chave de integração; o nome da UBS é descritivo.
# MAGIC 4. **Geometria:** cada `MultiGeometry` vira um `MULTIPOLYGON` em WKT. Cada polígono mantém o anel externo e todos os anéis internos (furos). As coordenadas são copiadas do texto original, sem arredondamento; a coordenada Z (sempre 0) é descartada e verificada.
# MAGIC 5. **Sistema de referência:** SIRGAS 2000 geográfico (`EPSG:4674`), conforme a descrição do documento KMZ. A geometria gravada **não é reprojetada**. A projeção `EPSG:31983` (SIRGAS 2000 / UTM 23S) é usada **apenas** para calcular a área e compará-la com `AREAKM2`.
# MAGIC 6. **Validação espacial:** funções espaciais nativas do Databricks (`st_geomfromwkt`, `st_isvalid`, `st_transform`, `st_area`, `st_point`, `st_covers`), sem bibliotecas adicionais.
# MAGIC 7. **Ponto x polígono:** o ponto de cada UBS vem de `ssp_silver_ubs_cadastro` (`geom_longitude`, `geom_latitude`), relacionado **somente pelo CNES**. `st_covers(área, ponto)` considera dentro também o ponto sobre a borda. O resultado fica em `status_ponto_poligono`: `DENTRO`, `FORA`, `SEM_COORDENADA` ou `GEOMETRIA_INVALIDA`. O ponto (WGS 84 no KML) é interpretado em SIRGAS 2000; a diferença entre os dois é desprezível nesta escala. Nenhuma coordenada ou geometria é corrigida.
# MAGIC
# MAGIC **Idempotência:** tabela vazia → grava; mesma versão da fonte (SHA-256) já gravada → não regrava; versão diferente → interrompe. Nada é sobrescrito.

# COMMAND ----------

# DBTITLE 1,1. Parâmetros do projeto
# Parâmetros (mesma nomenclatura dos notebooks 00, 01 e 02)
PREFIXO = "ssp"
CATALOGO = "workspace"
SCHEMA = "default"
TABELA_MANIFESTO = f"{CATALOGO}.{SCHEMA}.{PREFIXO}_bronze_manifest"
TABELA_CADASTRO = f"{CATALOGO}.{SCHEMA}.{PREFIXO}_silver_ubs_cadastro"      # somente leitura
TABELA_AREAS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}_silver_areas_abrangencia"

FONTE = "aaubs_2026_kmz"
ARQUIVO_INTERNO = "doc.kml"
QTD_ESPERADA = 485
KML_NS = {"k": "http://www.opengis.net/kml/2.2"}

# Chaves da tabela HTML de cada placemark -> colunas padronizadas
CAMPOS_ORIGEM = {
    "CNES_UBS": "cnes_original",
    "NOME_UBS": "nome_ubs",
    "STS_UBS": "sts_ubs",
    "CRS_UBS": "crs_ubs",
    "AREAKM2": "area_km2_texto",
}

# Sistema de referência (conforme descrição do documento: "Sistema de Coordenadas Geográficas - Datum: SIRGAS2000")
SRID_ORIGEM = 4674            # SIRGAS 2000 geográfico - SRID da geometria gravada
CRS_GEOMETRIA = "EPSG:4674 - SIRGAS 2000, coordenadas geográficas (lon/lat)"
SRID_AREA = 31983             # SIRGAS 2000 / UTM 23S - usado somente para calcular a área

TOLERANCIA_AREA_KM2 = 0.001   # diferença aceita entre área calculada e AREAKM2
# Envelope aproximado do Município de São Paulo, apenas para teste de plausibilidade
BBOX_MSP = {"lon_min": -46.83, "lon_max": -46.36, "lat_min": -24.01, "lat_max": -23.35}

print("Tabela de destino:", TABELA_AREAS)

# COMMAND ----------

# DBTITLE 1,2. Localização e verificação do KMZ na Bronze
import re, html, json, hashlib, uuid, datetime, zipfile
import xml.etree.ElementTree as ET
import pandas as pd

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

caminho_kmz = versoes[0]["caminho_bronze"]
sha_manifesto = versoes[0]["sha256"]
id_ingestao_bronze = spark.sql(
    f"""SELECT id_ingestao FROM {TABELA_MANIFESTO}
        WHERE fonte = :f AND sha256 = :h AND status = 'INGERIDO'
        ORDER BY data_ingestao LIMIT 1""",
    args={"f": FONTE, "h": sha_manifesto},
).first()["id_ingestao"]

# O arquivo no Volume deve ser exatamente o registrado na Bronze
sha_atual = sha256_arquivo(caminho_kmz)
assert sha_atual == sha_manifesto, "Arquivo Bronze difere do SHA-256 do manifesto; processamento interrompido"

print("Arquivo:", caminho_kmz)
print("SHA-256:", sha_atual, "| id_ingestao Bronze:", id_ingestao_bronze)

# COMMAND ----------

# DBTITLE 1,3. Leitura do KMZ, atributos HTML e geometrias
def texto_limpo(valor):
    """Remove tags HTML, decodifica entidades e devolve None para texto vazio."""
    if valor is None:
        return None
    texto = html.unescape(re.sub(r"<[^>]+>", "", valor)).strip()
    return texto if texto else None

def atributos_tabela_html(descricao):
    """Extrai os pares <td>CHAVE</td><td>valor</td>. Retorna ({chave: valor}, [chaves duplicadas])."""
    pares = re.findall(r"<td>\s*([A-Za-z0-9_]+)\s*</td>\s*<td>(.*?)</td>", descricao or "", flags=re.DOTALL)
    atributos, duplicadas = {}, []
    for chave, valor in pares:
        if chave in atributos:
            duplicadas.append(chave)
        atributos[chave] = texto_limpo(valor)
    return atributos, duplicadas

def geometria_multipoligono(placemark):
    """Converte todos os Polygon do placemark (anel externo + furos) em um MULTIPOLYGON WKT.
    Usa o texto original das coordenadas (lon lat), descartando Z."""
    poligonos_wkt, problemas = [], set()
    qtd_internos = qtd_vertices = 0
    altitude_nao_zero = False
    lons, lats = [], []
    poligonos = placemark.findall(".//k:Polygon", KML_NS)
    for poligono in poligonos:
        externo = poligono.find("k:outerBoundaryIs", KML_NS)
        internos = poligono.findall("k:innerBoundaryIs", KML_NS)
        if externo is None:
            problemas.add("SEM_ANEL_EXTERNO")
        aneis_wkt = []
        for anel in ([externo] if externo is not None else []) + internos:
            pontos = [p.split(",") for p in (anel.findtext(".//k:coordinates", namespaces=KML_NS) or "").split()]
            try:
                valores = [(float(p[0]), float(p[1]), float(p[2]) if len(p) > 2 else 0.0) for p in pontos]
            except (ValueError, IndexError):
                problemas.add("COORDENADA_INVALIDA")
                continue
            if len(pontos) < 4:
                problemas.add("ANEL_COM_MENOS_DE_4_PONTOS")
            elif pontos[0][:2] != pontos[-1][:2]:
                problemas.add("ANEL_NAO_FECHADO")
            altitude_nao_zero = altitude_nao_zero or any(v[2] != 0 for v in valores)
            lons += [v[0] for v in valores]
            lats += [v[1] for v in valores]
            qtd_vertices += len(pontos)
            aneis_wkt.append("(" + ", ".join(f"{p[0]} {p[1]}" for p in pontos) + ")")
        qtd_internos += len(internos)
        if aneis_wkt:
            poligonos_wkt.append("(" + ", ".join(aneis_wkt) + ")")
    if not poligonos:
        problemas.add("SEM_POLIGONO")
    wkt = f"MULTIPOLYGON ({', '.join(poligonos_wkt)})" if poligonos_wkt and not problemas else None
    fora_bbox = None if not lons else not (
        BBOX_MSP["lon_min"] <= min(lons) and max(lons) <= BBOX_MSP["lon_max"]
        and BBOX_MSP["lat_min"] <= min(lats) and max(lats) <= BBOX_MSP["lat_max"]
    )
    return {
        "geometria_wkt": wkt, "qtd_poligonos": len(poligonos), "qtd_aneis_internos": qtd_internos,
        "qtd_vertices": qtd_vertices, "problemas_geometria": sorted(problemas),
        "altitude_nao_zero": altitude_nao_zero, "geometria_fora_bbox": fora_bbox,
    }

# Leitura do KMZ (somente leitura)
with zipfile.ZipFile(caminho_kmz) as kmz:
    print("Arquivos no KMZ:", kmz.namelist())
    assert ARQUIVO_INTERNO in kmz.namelist(), f"{ARQUIVO_INTERNO} não encontrado no KMZ"
    conteudo = kmz.read(ARQUIVO_INTERNO)

# Correção apenas em memória: o doc.kml usa o prefixo xsi sem declará-lo
DECLARACAO_XSI = b'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
correcao_xsi = b"xsi:" in conteudo and DECLARACAO_XSI not in conteudo
if correcao_xsi:
    conteudo = conteudo.replace(b"<kml ", b"<kml " + DECLARACAO_XSI + b" ", 1)
print("Declaração xsi incluída em memória:", correcao_xsi)

raiz = ET.fromstring(conteudo)
placemarks = list(raiz.iter(f"{{{KML_NS['k']}}}Placemark"))

brutos = []
for indice, pm in enumerate(placemarks):
    descricao = pm.findtext("k:description", namespaces=KML_NS)
    atributos, duplicadas = atributos_tabela_html(descricao)
    tipos = [c.tag.split("}")[1] for c in pm if c.tag.split("}")[1] in ("MultiGeometry", "Polygon", "Point", "LineString")]
    brutos.append({
        "placemark_indice": indice,
        "nome_placemark": texto_limpo(pm.findtext("k:name", namespaces=KML_NS)),
        "tipo_geometria_origem": ",".join(tipos) or None,
        "descricao_html_original": descricao,
        "atributos": atributos,
        "chaves_duplicadas": duplicadas,
        **geometria_multipoligono(pm),
    })

frequencia_chaves = pd.Series([c for b in brutos for c in b["atributos"]]).value_counts()
print(f"Placemarks: {len(brutos)}")
print("Frequência das chaves:", frequencia_chaves.to_dict())
print("Tipos de geometria:", pd.Series([b["tipo_geometria_origem"] for b in brutos]).value_counts(dropna=False).to_dict())
print("Polígonos por área:", pd.Series([b["qtd_poligonos"] for b in brutos]).value_counts().sort_index().to_dict())
print("Áreas com furos:", sum(b["qtd_aneis_internos"] > 0 for b in brutos))

# COMMAND ----------

# DBTITLE 1,4. Padronização de colunas, CNES e área
def normalizar_cnes(valor):
    """CNES como texto de 7 dígitos (mesma regra do notebook 02). Retorna (cnes ou None, status)."""
    if valor is None:
        return None, "AUSENTE"
    v = re.sub(r"\.0+$", "", valor.strip())
    if not re.fullmatch(r"\d{1,7}", v):
        return None, "INVALIDO"
    return v.zfill(7), ("OK" if len(v) == 7 else "COMPLETADO_ZEROS")

def numero_decimal_br(texto):
    """Converte '4,297755' em float. Retorna (valor ou None, status)."""
    if texto is None:
        return None, "AUSENTE"
    t = texto.strip()
    if "," in t and "." in t:
        return None, "FORMATO_AMBIGUO"
    try:
        return float(t.replace(",", ".")), "OK"
    except ValueError:
        return None, "INVALIDO"

# Pontos das UBS do cadastro (notebook 02) - somente leitura; relacionamento exclusivamente por CNES
pontos_cadastro = {
    r["cnes"]: (r["geom_longitude"], r["geom_latitude"])
    for r in spark.table(TABELA_CADASTRO).select("cnes", "geom_longitude", "geom_latitude").collect()
}

registros = []
for b in brutos:
    attrs = b["atributos"]
    linha = {coluna: attrs.get(chave) for chave, coluna in CAMPOS_ORIGEM.items()}
    linha["cnes"], linha["status_cnes"] = normalizar_cnes(linha["cnes_original"])
    linha["area_km2"], linha["status_conversao_area"] = numero_decimal_br(linha["area_km2_texto"])
    linha["srid"] = SRID_ORIGEM
    linha["crs_geometria"] = CRS_GEOMETRIA
    linha["cnes_no_cadastro"] = linha["cnes"] in pontos_cadastro if linha["cnes"] else None
    # Coordenadas do ponto usadas apenas na validação (não são gravadas nesta tabela)
    linha["ponto_longitude"], linha["ponto_latitude"] = pontos_cadastro.get(linha["cnes"], (None, None))
    extras = {k: v for k, v in attrs.items() if k not in CAMPOS_ORIGEM}
    linha["atributos_extras_json"] = json.dumps(extras, ensure_ascii=False, sort_keys=True) if extras else None
    linha["chaves_ausentes"] = [k for k in CAMPOS_ORIGEM if k not in attrs]
    for campo in ("placemark_indice", "nome_placemark", "tipo_geometria_origem", "descricao_html_original",
                  "chaves_duplicadas", "geometria_wkt", "qtd_poligonos", "qtd_aneis_internos", "qtd_vertices",
                  "problemas_geometria", "altitude_nao_zero", "geometria_fora_bbox"):
        linha[campo] = b[campo]
    registros.append(linha)

print(f"Registros padronizados: {len(registros)}")
print("status_cnes:", pd.Series([r["status_cnes"] for r in registros]).value_counts().to_dict())
print("status_conversao_area:", pd.Series([r["status_conversao_area"] for r in registros]).value_counts().to_dict())

# COMMAND ----------

# DBTITLE 1,5. Validação espacial (validade, área e ponto x polígono)
# DataFrame temporário em memória (não é gravado): índice do placemark + WKT + ponto da UBS (pelo CNES)
df_geo = spark.createDataFrame(
    [(r["placemark_indice"], r["geometria_wkt"], r["ponto_longitude"], r["ponto_latitude"])
     for r in registros if r["geometria_wkt"]],
    "placemark_indice INT, wkt STRING, lon DOUBLE, lat DOUBLE",
)
resultado_geo = (
    df_geo.selectExpr(
        "placemark_indice",
        f"st_geomfromwkt(wkt, {SRID_ORIGEM}) AS g",
        f"CASE WHEN lon IS NOT NULL AND lat IS NOT NULL THEN st_point(lon, lat, {SRID_ORIGEM}) END AS p",
    )
    .selectExpr(
        "placemark_indice",
        "st_isvalid(g) AS geometria_valida",
        f"st_area(st_transform(g, {SRID_AREA})) / 1e6 AS area_calculada_km2",
        # st_covers: verdadeiro para ponto no interior ou sobre a borda; só avaliado em geometria válida
        "CASE WHEN st_isvalid(g) THEN st_covers(g, p) END AS ponto_coberto",
    )
    .collect()
)
por_indice = {x["placemark_indice"]: x for x in resultado_geo}

for r in registros:
    x = por_indice.get(r["placemark_indice"])
    r["geometria_valida"] = x["geometria_valida"] if x else None
    r["area_calculada_km2"] = x["area_calculada_km2"] if x else None
    if r["area_km2"] is None:
        r["diferenca_area_km2"], r["status_area"] = None, "SEM_ATRIBUTO"
    elif r["area_calculada_km2"] is None:
        r["diferenca_area_km2"], r["status_area"] = None, "SEM_GEOMETRIA"
    else:
        r["diferenca_area_km2"] = r["area_calculada_km2"] - r["area_km2"]
        r["status_area"] = "OK" if abs(r["diferenca_area_km2"]) <= TOLERANCIA_AREA_KM2 else "DIVERGENTE"

    # Ponto da UBS x polígono da sua área (relacionados pelo CNES)
    if r["geometria_valida"] is not True:
        r["status_ponto_poligono"] = "GEOMETRIA_INVALIDA"
    elif r["ponto_longitude"] is None or r["ponto_latitude"] is None:
        r["status_ponto_poligono"] = "SEM_COORDENADA"
    else:
        r["status_ponto_poligono"] = "DENTRO" if x["ponto_coberto"] else "FORA"

df_espacial = pd.DataFrame([{
    "cnes": r["cnes"], "placemark_indice": r["placemark_indice"], "qtd_poligonos": r["qtd_poligonos"],
    "qtd_aneis_internos": r["qtd_aneis_internos"], "geometria_valida": r["geometria_valida"],
    "area_km2": r["area_km2"], "area_calculada_km2": r["area_calculada_km2"],
    "diferenca_area_km2": r["diferenca_area_km2"], "status_area": r["status_area"],
    "geometria_fora_bbox": r["geometria_fora_bbox"], "status_ponto_poligono": r["status_ponto_poligono"],
    "ponto_longitude": r["ponto_longitude"], "ponto_latitude": r["ponto_latitude"],
} for r in registros])

# Resumo (sempre há linhas: 485 registros)
display(df_espacial.groupby(["geometria_valida", "status_area"], dropna=False)
        .agg(qtd=("placemark_indice", "size"),
             dif_area_max_km2=("diferenca_area_km2", lambda s: s.abs().max()))
        .reset_index())

# MultiPolígonos e áreas com furos (informativo)
display(df_espacial[(df_espacial.qtd_poligonos > 1) | (df_espacial.qtd_aneis_internos > 0)]
        [["cnes", "qtd_poligonos", "qtd_aneis_internos", "area_km2", "area_calculada_km2"]])

# Ponto x polígono: resumo e pontos que não estão DENTRO (exibidos só se existirem)
display(df_espacial.groupby("status_ponto_poligono").size().reset_index(name="qtd"))
df_ponto_atencao = df_espacial[df_espacial.status_ponto_poligono != "DENTRO"]
if df_ponto_atencao.empty:
    print("Todos os pontos das UBS estão dentro (ou na borda) da sua área de abrangência.")
else:
    print(f"Pontos fora da própria área ou sem avaliação: {len(df_ponto_atencao)} (nada foi corrigido)")
    display(df_ponto_atencao[["cnes", "status_ponto_poligono", "ponto_longitude", "ponto_latitude", "area_km2"]])

# Registros que exigem atenção (exibidos só se existirem)
df_geo_atencao = df_espacial[(df_espacial.geometria_valida != True) | (df_espacial.status_area != "OK")
                             | (df_espacial.geometria_fora_bbox == True)]
if df_geo_atencao.empty:
    print("Todas as geometrias são válidas, com área compatível com AREAKM2 e dentro do envelope do MSP.")
else:
    print(f"Geometrias que exigem atenção: {len(df_geo_atencao)} (nenhuma geometria foi corrigida)")
    display(df_geo_atencao)

# COMMAND ----------

# DBTITLE 1,6. Regras de validação (bloqueantes, alertas e informativas)
import collections

id_execucao = str(uuid.uuid4())
data_execucao = datetime.datetime.now(datetime.timezone.utc)

def identificador(r):
    return r["cnes"] or f"placemark_{r['placemark_indice']}"

regras = []
def registrar(regra, descricao, severidade, afetados):
    regras.append({
        "regra": regra, "descricao": descricao, "severidade": severidade,
        "qtd_afetados": len(afetados), "aprovado": len(afetados) == 0,
        "afetados": [str(a) for a in afetados],
    })

contagem_cnes = collections.Counter(r["cnes"] for r in registros if r["cnes"])

# Bloqueantes: impedem a gravação
registrar("QTD_REGISTROS", f"Quantidade de áreas deve ser {QTD_ESPERADA} (encontradas {len(registros)})",
          "BLOQUEANTE", [] if len(registros) == QTD_ESPERADA else [f"encontradas={len(registros)}"])
registrar("CNES_VALIDO", "CNES presente e com 1 a 7 dígitos", "BLOQUEANTE",
          [identificador(r) for r in registros if r["cnes"] is None])
registrar("CNES_UNICO", "CNES não pode se repetir", "BLOQUEANTE",
          sorted(c for c, n in contagem_cnes.items() if n > 1))
registrar("GEOMETRIA_ESTRUTURA", "Polígonos presentes, anéis fechados e com 4 ou mais pontos", "BLOQUEANTE",
          [identificador(r) for r in registros if r["geometria_wkt"] is None])
registrar("GEOMETRIA_VALIDA", "Geometria válida segundo st_isvalid", "BLOQUEANTE",
          [identificador(r) for r in registros if r["geometria_valida"] is not True])

# Alertas: registrados, não impedem a gravação
registrar("CNES_COMPLETADO_ZEROS", "CNES com menos de 7 dígitos completado com zeros", "ALERTA",
          [identificador(r) for r in registros if r["status_cnes"] == "COMPLETADO_ZEROS"])
registrar("CNES_SEM_CADASTRO", "CNES ausente em ssp_silver_ubs_cadastro", "ALERTA",
          [identificador(r) for r in registros if r["cnes_no_cadastro"] is False])
registrar("CHAVES_AUSENTES", "Registro sem alguma chave esperada na tabela HTML", "ALERTA",
          [identificador(r) for r in registros if r["chaves_ausentes"]])
registrar("CHAVES_EXTRAS", "Chaves não mapeadas (preservadas em atributos_extras_json)", "ALERTA",
          [identificador(r) for r in registros if r["atributos_extras_json"]])
registrar("CHAVES_DUPLICADAS", "Chave repetida na mesma descrição (mantido o último valor)", "ALERTA",
          [identificador(r) for r in registros if r["chaves_duplicadas"]])
registrar("AREA_NAO_CONVERTIDA", "AREAKM2 ausente ou não conversível", "ALERTA",
          [identificador(r) for r in registros if r["status_conversao_area"] != "OK"])
registrar("AREA_DIVERGENTE", f"Área calculada difere de AREAKM2 em mais de {TOLERANCIA_AREA_KM2} km²", "ALERTA",
          [identificador(r) for r in registros if r["status_area"] == "DIVERGENTE"])
registrar("GEOMETRIA_FORA_BBOX", "Área com vértice fora do envelope aproximado do MSP", "ALERTA",
          [identificador(r) for r in registros if r["geometria_fora_bbox"]])
registrar("ALTITUDE_NAO_ZERO", "Coordenada Z diferente de 0 (descartada no WKT)", "ALERTA",
          [identificador(r) for r in registros if r["altitude_nao_zero"]])
registrar("PONTO_FORA_DA_AREA", "Ponto da UBS (cadastro) fora da própria área segundo st_covers", "ALERTA",
          [identificador(r) for r in registros if r["status_ponto_poligono"] == "FORA"])
registrar("PONTO_SEM_COORDENADA", "CNES sem coordenada no cadastro (ponto x polígono não avaliado)", "ALERTA",
          [identificador(r) for r in registros if r["status_ponto_poligono"] == "SEM_COORDENADA"])
registrar("TIPO_GEOMETRIA_INESPERADO", "Elemento de geometria diferente de MultiGeometry", "ALERTA",
          [identificador(r) for r in registros if r["tipo_geometria_origem"] != "MultiGeometry"])

# Informativas: características das geometrias e valores vazios
registrar("MULTIPOLIGONO", "Áreas com mais de um polígono (preservados no MULTIPOLYGON)", "INFO",
          [identificador(r) for r in registros if r["qtd_poligonos"] > 1])
registrar("COM_FUROS", "Áreas com anéis internos (preservados no WKT)", "INFO",
          [identificador(r) for r in registros if r["qtd_aneis_internos"] > 0])
for coluna in CAMPOS_ORIGEM.values():
    registrar(f"VAZIO_{coluna.upper()}", f"Valores vazios em {coluna} (mantidos como NULL)", "INFO",
              [identificador(r) for r in registros if r[coluna] is None])

# Resultado das validações (exibido no notebook; não é gravado em tabela separada)
df_regras = pd.DataFrame(regras)
display(df_regras[["regra", "severidade", "qtd_afetados", "aprovado", "descricao"]])

df_afetados = (df_regras[(df_regras.severidade != "INFO") & (~df_regras.aprovado)]
               [["regra", "severidade", "afetados"]].explode("afetados"))
if df_afetados.empty:
    print("Nenhum registro afetado por regras bloqueantes ou de alerta.")
else:
    display(df_afetados.rename(columns={"afetados": "cnes_ou_placemark"}))

bloqueios = df_regras[(df_regras.severidade == "BLOQUEANTE") & (~df_regras.aprovado)]
if not bloqueios.empty:
    raise RuntimeError(f"Regras bloqueantes reprovadas: {bloqueios.regra.tolist()}. Nada foi gravado.")
print("Regras bloqueantes: APROVADAS")

# COMMAND ----------

# DBTITLE 1,7. Criação não destrutiva da tabela Silver
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, BooleanType,
                               IntegerType, TimestampType, ArrayType)

# Definição única das colunas: gera o DDL da tabela e o schema do DataFrame (mesma ordem e tipos)
COLUNAS = [
    ("cnes", "STRING", False, "CNES normalizado (texto, 7 dígitos) - chave de integração"),
    ("cnes_original", "STRING", True, "CNES_UBS como na origem"),
    ("status_cnes", "STRING", True, "OK ou COMPLETADO_ZEROS"),
    ("nome_ubs", "STRING", True, "NOME_UBS (descritivo; não é chave)"),
    ("sts_ubs", "STRING", True, "STS_UBS como na origem"),
    ("crs_ubs", "STRING", True, "CRS_UBS como na origem"),
    ("area_km2_texto", "STRING", True, "AREAKM2 como na origem"),
    ("area_km2", "DOUBLE", True, "AREAKM2 convertido (área oficial informada)"),
    ("status_conversao_area", "STRING", True, "Resultado da conversão de AREAKM2"),
    ("area_calculada_km2", "DOUBLE", True, "Área calculada da geometria em EPSG:31983 (apenas validação)"),
    ("diferenca_area_km2", "DOUBLE", True, "area_calculada_km2 - area_km2"),
    ("status_area", "STRING", True, "OK, DIVERGENTE, SEM_ATRIBUTO ou SEM_GEOMETRIA"),
    ("geometria_wkt", "STRING", True, "MULTIPOLYGON em WKT (lon lat), texto original das coordenadas, sem Z"),
    ("srid", "INT", True, "SRID da geometria (4674 = SIRGAS 2000 geográfico)"),
    ("crs_geometria", "STRING", True, "Descrição do sistema de referência da geometria"),
    ("tipo_geometria_origem", "STRING", True, "Elemento de geometria no KML de origem"),
    ("qtd_poligonos", "INT", True, "Quantidade de polígonos"),
    ("qtd_aneis_internos", "INT", True, "Quantidade de anéis internos (furos)"),
    ("qtd_vertices", "INT", True, "Quantidade total de vértices"),
    ("problemas_geometria", "ARRAY<STRING>", True, "Problemas estruturais encontrados (vazio = nenhum)"),
    ("geometria_valida", "BOOLEAN", True, "Resultado de st_isvalid"),
    ("geometria_fora_bbox", "BOOLEAN", True, "Vértice fora do envelope aproximado do MSP"),
    ("altitude_nao_zero", "BOOLEAN", True, "Alguma coordenada com Z diferente de 0"),
    ("cnes_no_cadastro", "BOOLEAN", True, "CNES existe em ssp_silver_ubs_cadastro"),
    ("status_ponto_poligono", "STRING", True, "DENTRO, FORA, SEM_COORDENADA ou GEOMETRIA_INVALIDA (st_covers do ponto do cadastro na área, por CNES)"),
    ("chaves_ausentes", "ARRAY<STRING>", True, "Chaves esperadas ausentes na tabela HTML"),
    ("chaves_duplicadas", "ARRAY<STRING>", True, "Chaves repetidas na tabela HTML"),
    ("atributos_extras_json", "STRING", True, "Chaves não mapeadas (JSON)"),
    ("placemark_indice", "INT", True, "Posição do placemark no doc.kml"),
    ("nome_placemark", "STRING", True, "Nome do placemark (descritivo; não é chave)"),
    ("descricao_html_original", "STRING", True, "Descrição HTML original (rastreabilidade)"),
    ("fonte_arquivo", "STRING", True, "Caminho do KMZ na Bronze"),
    ("arquivo_interno", "STRING", True, "Arquivo KML lido dentro do KMZ"),
    ("fonte_sha256", "STRING", False, "SHA-256 do arquivo Bronze processado"),
    ("id_ingestao_bronze", "STRING", True, "id_ingestao do manifesto Bronze"),
    ("id_execucao", "STRING", True, "Identificador desta execução do notebook 03"),
    ("data_processamento", "TIMESTAMP", True, "Data/hora do processamento (UTC)"),
]
TIPOS_SPARK = {"STRING": StringType(), "DOUBLE": DoubleType(), "INT": IntegerType(), "BOOLEAN": BooleanType(),
               "TIMESTAMP": TimestampType(), "ARRAY<STRING>": ArrayType(StringType())}

colunas_ddl = ",\n    ".join(
    f"{nome} {tipo}{'' if nulo else ' NOT NULL'} COMMENT '{comentario}'" for nome, tipo, nulo, comentario in COLUNAS
)

# IF NOT EXISTS: se a tabela já existir, nada é alterado.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABELA_AREAS} (
    {colunas_ddl}
)
USING DELTA
COMMENT 'Projeto Saúde Perto SP - Silver: áreas de abrangência das UBS (KMZ oficial 2026, SIRGAS 2000). Chave: cnes.'
""")
print("Tabela disponível:", TABELA_AREAS)

# COMMAND ----------

# DBTITLE 1,8. Gravação idempotente das áreas
schema_areas = StructType([StructField(n, TIPOS_SPARK[t], nulo) for n, t, nulo, _ in COLUNAS])

rastreio = {"fonte_arquivo": caminho_kmz, "arquivo_interno": ARQUIVO_INTERNO, "fonte_sha256": sha_atual,
            "id_ingestao_bronze": id_ingestao_bronze, "id_execucao": id_execucao, "data_processamento": data_execucao}
linhas = [tuple({**r, **rastreio}[n] for n, _, _, _ in COLUNAS) for r in registros]

# Conferir que o esquema da tabela existente coincide com o esperado (evita gravação incompatível)
colunas_tabela = [c.name for c in spark.table(TABELA_AREAS).schema]
assert colunas_tabela == [n for n, _, _, _ in COLUNAS], "Esquema da tabela difere do esperado; gravação interrompida"

# Idempotência baseada no SHA-256 da fonte
estado = spark.sql(f"SELECT count(*) AS n, collect_set(fonte_sha256) AS versoes FROM {TABELA_AREAS}").first()

if estado["n"] == 0:
    spark.createDataFrame(linhas, schema=schema_areas).write.mode("append").saveAsTable(TABELA_AREAS)
    status_gravacao = "GRAVADO"
elif list(estado["versoes"]) == [sha_atual] and estado["n"] == len(linhas):
    status_gravacao = "JA_PROCESSADO"
else:
    raise RuntimeError(
        f"{TABELA_AREAS} já contém {estado['n']} linha(s) de versão(ões) {list(estado['versoes'])}, "
        f"diferente do processamento atual ({len(linhas)} linhas, SHA-256 {sha_atual}). "
        "Nada foi gravado; reprocessamento exige decisão explícita."
    )

print(f"Áreas: {status_gravacao} | linhas preparadas: {len(linhas)} | id_execucao = {id_execucao}")

# COMMAND ----------

# DBTITLE 1,9. Validação pós-gravação
# Conferência independente, lendo a tabela gravada e reconstruindo as geometrias a partir do WKT
resumo = spark.sql(f"""
    SELECT count(*)                                                AS linhas,
           count(DISTINCT cnes)                                    AS cnes_distintos,
           count_if(cnes RLIKE '^[0-9]{{7}}$')                     AS cnes_7_digitos,
           count_if(st_isvalid(st_geomfromwkt(geometria_wkt, {SRID_ORIGEM}))) AS geometrias_validas,
           count_if(srid = {SRID_ORIGEM})                          AS srid_origem,
           count(DISTINCT fonte_sha256)                            AS versoes_fonte,
           max(fonte_sha256)                                       AS fonte_sha256
    FROM {TABELA_AREAS}
""").first()
print(resumo.asDict())

assert resumo["linhas"] == QTD_ESPERADA, f"Esperadas {QTD_ESPERADA} linhas, encontradas {resumo['linhas']}"
assert resumo["cnes_distintos"] == resumo["linhas"], "CNES duplicado na tabela gravada"
assert resumo["cnes_7_digitos"] == resumo["linhas"], "Existe CNES fora do padrão de 7 dígitos"
assert resumo["geometrias_validas"] == resumo["linhas"], "Existe geometria inválida na tabela gravada"
assert resumo["srid_origem"] == resumo["linhas"], "Existe SRID diferente do original"
assert resumo["versoes_fonte"] == 1 and resumo["fonte_sha256"] == sha_atual, "Versão da fonte na tabela difere da Bronze"

# Preparação para integração: correspondência de CNES com o cadastro (somente leitura)
display(spark.sql(f"""
    SELECT count_if(a.cnes IS NOT NULL AND c.cnes IS NOT NULL) AS cnes_em_ambas,
           count_if(c.cnes IS NULL)                            AS somente_areas,
           count_if(a.cnes IS NULL)                            AS somente_cadastro
    FROM {TABELA_AREAS} a FULL OUTER JOIN {TABELA_CADASTRO} c ON a.cnes = c.cnes
"""))

# Ponto x polígono: recalcula a partir das duas tabelas gravadas e compara com o status armazenado
ponto = spark.sql(f"""
    SELECT a.status_ponto_poligono,
           CASE WHEN NOT st_isvalid(st_geomfromwkt(a.geometria_wkt, {SRID_ORIGEM})) THEN 'GEOMETRIA_INVALIDA'
                WHEN c.geom_longitude IS NULL OR c.geom_latitude IS NULL THEN 'SEM_COORDENADA'
                WHEN st_covers(st_geomfromwkt(a.geometria_wkt, {SRID_ORIGEM}),
                               st_point(c.geom_longitude, c.geom_latitude, {SRID_ORIGEM})) THEN 'DENTRO'
                ELSE 'FORA' END AS recalculado
    FROM {TABELA_AREAS} a LEFT JOIN {TABELA_CADASTRO} c ON a.cnes = c.cnes
""")
assert ponto.filter("NOT (status_ponto_poligono <=> recalculado)").count() == 0, "status_ponto_poligono difere do recalculado"
display(ponto.groupBy("status_ponto_poligono").count().orderBy("status_ponto_poligono"))

display(spark.sql(f"""
    SELECT status_area, qtd_poligonos, count(*) AS qtd, count_if(qtd_aneis_internos > 0) AS com_furos,
           round(max(abs(diferenca_area_km2)), 6) AS dif_area_max_km2
    FROM {TABELA_AREAS} GROUP BY status_area, qtd_poligonos ORDER BY status_area, qtd_poligonos
"""))

display(spark.sql(f"""
    SELECT cnes, nome_ubs, sts_ubs, crs_ubs, area_km2, round(area_calculada_km2, 6) AS area_calculada_km2,
           qtd_poligonos, qtd_aneis_internos, qtd_vertices, srid
    FROM {TABELA_AREAS} ORDER BY cnes LIMIT 5
"""))
print("Silver áreas de abrangência: APROVADA")

# COMMAND ----------

# DBTITLE 1,Resumo e próximos passos
# MAGIC %md
# MAGIC ## Resultado esperado
# MAGIC * 485 linhas em `ssp_silver_areas_abrangencia`, 485 CNES distintos com 7 dígitos, todas as geometrias válidas e com SRID 4674.
# MAGIC * MultiPolígonos e furos preservados no WKT; área calculada compatível com `AREAKM2`.
# MAGIC * 485 CNES correspondentes em `ssp_silver_ubs_cadastro`, prontos para integração por `cnes`.
# MAGIC * `status_ponto_poligono` preenchido para as 485 áreas; pontos `FORA` ficam registrados como alerta, sem correção.
# MAGIC * Reexecução com a mesma versão da fonte: `status_gravacao = JA_PROCESSADO`, sem nova gravação.
# MAGIC
# MAGIC ## Uso posterior da geometria
# MAGIC `st_geomfromwkt(geometria_wkt, 4674)` reconstrói a geometria em SIRGAS 2000 para as etapas seguintes.