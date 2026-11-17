# Databricks notebook source
# DBTITLE 1,Apresentação do notebook
# MAGIC %md
# MAGIC # 02 — Silver: cadastro das UBS | Projeto Saúde Perto SP
# MAGIC
# MAGIC **Objetivo:** estruturar o cadastro das 485 unidades de saúde a partir dos **pontos** do arquivo `Consulta UBS.kml`, lido da cópia Bronze no Volume. Sem camada Gold, indicadores ou dashboard.
# MAGIC
# MAGIC **Entrada:** pasta KML `Estabelecimento de Saúde Referencia da AA-UBS` (485 *placemarks* do tipo `Point`). Os polígonos do KML são tratados no notebook 03.
# MAGIC
# MAGIC **Saída:** `workspace.default.ssp_silver_ubs_cadastro`, uma linha por CNES. Os resultados das validações são exibidos no próprio notebook, e as inconsistências por registro ficam nas colunas de status da tabela (`status_cnes`, `status_conversao_coord_atributo`, `status_coordenada`, `distancia_geom_atributo_m`, `geometria_fora_bbox`, `cep_formato_valido`, `chaves_ausentes`, `chaves_duplicadas`). Não há tabela de qualidade separada nesta etapa.
# MAGIC
# MAGIC **Regras de tratamento**
# MAGIC 1. **Descrição HTML:** dividida por `<br>`; o 1º segmento é o título; cada segmento seguinte é `CHAVE  valor` (chave = primeiro token sem espaços). Tags HTML removidas e entidades decodificadas. O HTML original é preservado em `descricao_html_original`.
# MAGIC 2. **Valores vazios:** texto vazio após `strip` vira `NULL`. Nenhum valor é inferido ou preenchido.
# MAGIC 3. **CNES:** texto; remove apenas espaços e o sufixo `.0`; aceita somente 1 a 7 dígitos; completa zeros à esquerda até 7 dígitos (marcado em `status_cnes`). Qualquer outro formato é inválido e bloqueia a gravação.
# MAGIC 4. **Coordenadas:** a geometria do KML é mantida como está (lon/lat, `EPSG:4326` pelo padrão KML 2.2). `LONSIRGAS`/`LATSIRGAS` são convertidos de vírgula decimal para número (`EPSG:4674`, SIRGAS 2000) e comparados à geometria pela distância de grande círculo (haversine, em metros): até 1 m `IDENTICA`; acima de 1 até 50 m `PROXIMA`; acima de 50 m `DIVERGENTE` (com verificação de possível inversão lon/lat). Nada é corrigido.
# MAGIC 5. **Chave de integração:** somente `cnes`. Nomes de UBS são atributos descritivos.
# MAGIC
# MAGIC **Idempotência:** tabela vazia → grava; mesma versão da fonte (SHA-256) já gravada → não regrava; versão diferente → interrompe para decisão explícita. Nada é sobrescrito.

# COMMAND ----------

# DBTITLE 1,1. Parâmetros do projeto
# Parâmetros (mesma nomenclatura dos notebooks 00 e 01)
PREFIXO = "ssp"
CATALOGO = "workspace"
SCHEMA = "default"
TABELA_MANIFESTO = f"{CATALOGO}.{SCHEMA}.{PREFIXO}_bronze_manifest"
TABELA_CADASTRO = f"{CATALOGO}.{SCHEMA}.{PREFIXO}_silver_ubs_cadastro"

FONTE = "consulta_ubs_kml"
PASTA_PONTOS = "Estabelecimento de Saúde Referencia da AA-UBS"  # nome exato da pasta no KML (inspeção)
QTD_ESPERADA = 485
KML_NS = {"k": "http://www.opengis.net/kml/2.2"}

# Chaves encontradas na descrição HTML dos pontos (inspeção) -> colunas padronizadas (snake_case)
CAMPOS_ORIGEM = {
    "CNES": "cnes_original",
    "Nome_Divul": "nome_divulgacao",
    "Fone_1": "telefone_1",
    "Fone_2": "telefone_2",
    "Logradouro": "logradouro",
    "Numero_Log": "numero_logradouro",
    "Complement": "complemento",
    "Bairro": "bairro",
    "CEP": "cep",
    "Obs_Endere": "observacao_endereco",
    "Horario_Fu": "horario_funcionamento",
    "LONSIRGAS": "longitude_atributo_texto",
    "LATSIRGAS": "latitude_atributo_texto",
    "SubPref_": "subprefeitura",
    "STS": "supervisao_tecnica_saude",
    "UVIS": "uvis",
    "CRS": "coordenadoria_regional_saude",
}

# Referências de coordenadas (nenhuma reprojeção é feita neste notebook)
CRS_GEOMETRIA = "EPSG:4326"   # KML 2.2: longitude/latitude WGS 84
CRS_ATRIBUTOS = "EPSG:4674"   # LONSIRGAS/LATSIRGAS: SIRGAS 2000 (conforme o nome do atributo)

# Validação de coordenadas (somente classificação; nada é corrigido)
LIMITE_IDENTICA_M = 1.0       # até 1 m: IDENTICA
TOLERANCIA_COORD_M = 50.0     # até 50 m: PROXIMA; acima: DIVERGENTE
# Envelope aproximado do Município de São Paulo, apenas para teste de plausibilidade
BBOX_MSP = {"lon_min": -46.83, "lon_max": -46.36, "lat_min": -24.01, "lat_max": -23.35}

print("Tabela de destino:", TABELA_CADASTRO)

# COMMAND ----------

# DBTITLE 1,2. Localização e verificação do KML na Bronze
import os, re, html, json, math, hashlib, uuid, datetime
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

caminho_kml = versoes[0]["caminho_bronze"]
sha_manifesto = versoes[0]["sha256"]
id_ingestao_bronze = spark.sql(
    f"""SELECT id_ingestao FROM {TABELA_MANIFESTO}
        WHERE fonte = :f AND sha256 = :h AND status = 'INGERIDO'
        ORDER BY data_ingestao LIMIT 1""",
    args={"f": FONTE, "h": sha_manifesto},
).first()["id_ingestao"]

# O arquivo no Volume deve ser exatamente o registrado na Bronze
sha_atual = sha256_arquivo(caminho_kml)
assert sha_atual == sha_manifesto, "Arquivo Bronze difere do SHA-256 do manifesto; processamento interrompido"

print("Arquivo:", caminho_kml)
print("SHA-256:", sha_atual, "| id_ingestao Bronze:", id_ingestao_bronze)

# COMMAND ----------

# DBTITLE 1,3. Leitura dos pontos do KML e parsing das descrições HTML
def texto_limpo(valor):
    """Remove tags HTML, decodifica entidades e devolve None para texto vazio."""
    if valor is None:
        return None
    texto = html.unescape(re.sub(r"<[^>]+>", "", valor)).strip()
    return texto if texto else None

def parse_descricao(descricao):
    """Descrição no formato 'TÍTULO <br> <br> CHAVE   valor <br> ...'.
    Retorna (título, {chave: valor ou None}, [chaves duplicadas])."""
    segmentos = re.split(r"<br\s*/?>", descricao or "", flags=re.IGNORECASE)
    titulo = texto_limpo(segmentos[0]) if segmentos else None
    atributos, duplicadas = {}, []
    for segmento in segmentos[1:]:
        texto = html.unescape(re.sub(r"<[^>]+>", "", segmento)).strip()
        if not texto:
            continue
        correspondencia = re.match(r"(\S+)\s*(.*)", texto, flags=re.DOTALL)
        chave, valor = correspondencia.group(1), correspondencia.group(2).strip()
        if chave in atributos:
            duplicadas.append(chave)
        atributos[chave] = valor if valor else None
    return titulo, atributos, duplicadas

raiz = ET.parse(caminho_kml).getroot()  # leitura apenas; o arquivo não é modificado
pastas = [
    f for f in raiz.iter(f"{{{KML_NS['k']}}}Folder")
    if (f.findtext("k:name", namespaces=KML_NS) or "").strip() == PASTA_PONTOS
]
assert len(pastas) == 1, f"Pasta '{PASTA_PONTOS}' encontrada {len(pastas)} vez(es); esperado 1"
placemarks = pastas[0].findall("k:Placemark", KML_NS)

brutos = []
for indice, pm in enumerate(placemarks):
    pontos = pm.findall(".//k:Point", KML_NS)
    poligonos = pm.findall(".//k:Polygon", KML_NS)
    coord_texto = (pontos[0].findtext("k:coordinates", namespaces=KML_NS) or "").strip() if len(pontos) == 1 else None
    lon_txt = lat_txt = alt_txt = None
    if coord_texto:
        partes = coord_texto.split(",")
        lon_txt = partes[0] if len(partes) > 0 else None
        lat_txt = partes[1] if len(partes) > 1 else None
        alt_txt = partes[2] if len(partes) > 2 else None
    descricao = pm.findtext("k:description", namespaces=KML_NS)
    titulo, atributos, duplicadas = parse_descricao(descricao)
    brutos.append({
        "placemark_indice": indice,
        "nome_placemark": texto_limpo(pm.findtext("k:name", namespaces=KML_NS)),
        "titulo_descricao": titulo,
        "style_url": texto_limpo(pm.findtext("k:styleUrl", namespaces=KML_NS)),
        "qtd_pontos": len(pontos),
        "qtd_poligonos": len(poligonos),
        "coordenadas_texto": coord_texto,
        "lon_txt": lon_txt, "lat_txt": lat_txt, "alt_txt": alt_txt,
        "descricao_html_original": descricao,
        "atributos": atributos,
        "chaves_duplicadas": duplicadas,
    })

frequencia_chaves = pd.Series([c for b in brutos for c in b["atributos"]]).value_counts()
print(f"Placemarks na pasta: {len(brutos)}")
print("Frequência das chaves encontradas nas descrições:")
print(frequencia_chaves.to_string())
print("Chaves não mapeadas:", sorted(set(frequencia_chaves.index) - set(CAMPOS_ORIGEM)))
print("Chaves esperadas ausentes em todos os registros:", sorted(set(CAMPOS_ORIGEM) - set(frequencia_chaves.index)))

# COMMAND ----------

# DBTITLE 1,4. Padronização de colunas, CNES e valores vazios
def normalizar_cnes(valor):
    """CNES como texto de 7 dígitos. Retorna (cnes ou None, status)."""
    if valor is None:
        return None, "AUSENTE"
    v = re.sub(r"\.0+$", "", valor.strip())  # artefato de número exportado como decimal
    if not re.fullmatch(r"\d{1,7}", v):
        return None, "INVALIDO"
    return v.zfill(7), ("OK" if len(v) == 7 else "COMPLETADO_ZEROS")

def numero_decimal_br(texto):
    """Converte '-46,46926' em float. Retorna (valor ou None, status)."""
    if texto is None:
        return None, "AUSENTE"
    t = texto.strip()
    if "," in t and "." in t:
        return None, "FORMATO_AMBIGUO"
    try:
        return float(t.replace(",", ".")), "OK"
    except ValueError:
        return None, "INVALIDO"

def numero_kml(texto):
    try:
        return float(texto) if texto is not None else None
    except ValueError:
        return None

registros = []
for b in brutos:
    attrs = b["atributos"]
    linha = {coluna: attrs.get(chave) for chave, coluna in CAMPOS_ORIGEM.items()}
    linha["cnes"], linha["status_cnes"] = normalizar_cnes(linha["cnes_original"])
    linha["longitude_atributo"], st_lon = numero_decimal_br(linha["longitude_atributo_texto"])
    linha["latitude_atributo"], st_lat = numero_decimal_br(linha["latitude_atributo_texto"])
    linha["status_conversao_coord_atributo"] = "OK" if (st_lon, st_lat) == ("OK", "OK") else f"LON_{st_lon}|LAT_{st_lat}"
    linha["cep_formato_valido"] = bool(re.fullmatch(r"\d{5}-\d{3}", linha["cep"])) if linha["cep"] else None

    linha["geom_longitude"] = numero_kml(b["lon_txt"])
    linha["geom_latitude"] = numero_kml(b["lat_txt"])
    linha["geom_altitude"] = numero_kml(b["alt_txt"])
    # WKT montado com o texto original das coordenadas (sem perda de precisão)
    linha["geometria_wkt"] = (
        f"POINT ({b['lon_txt'].strip()} {b['lat_txt'].strip()})"
        if linha["geom_longitude"] is not None and linha["geom_latitude"] is not None else None
    )
    linha["crs_geometria"] = CRS_GEOMETRIA
    linha["crs_atributos"] = CRS_ATRIBUTOS

    extras = {k: v for k, v in attrs.items() if k not in CAMPOS_ORIGEM}
    linha["atributos_extras_json"] = json.dumps(extras, ensure_ascii=False, sort_keys=True) if extras else None
    linha["chaves_ausentes"] = [k for k in CAMPOS_ORIGEM if k not in attrs]
    linha["chaves_duplicadas"] = b["chaves_duplicadas"]

    for campo in ("placemark_indice", "nome_placemark", "titulo_descricao", "style_url",
                  "qtd_pontos", "qtd_poligonos", "coordenadas_texto", "descricao_html_original"):
        linha[campo] = b[campo]
    registros.append(linha)

print(f"Registros padronizados: {len(registros)}")
print("status_cnes:", pd.Series([r['status_cnes'] for r in registros]).value_counts().to_dict())

# COMMAND ----------

# DBTITLE 1,5. Validação das coordenadas (geometria x LONSIRGAS/LATSIRGAS)
RAIO_TERRA_M = 6371008.8  # raio médio (IUGG)

def distancia_haversine_m(lon1, lat1, lon2, lat2):
    """Distância de grande círculo em metros (adequada para diferenças de poucos metros a km)."""
    if None in (lon1, lat1, lon2, lat2):
        return None
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat, dlon = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * RAIO_TERRA_M * math.asin(math.sqrt(a))

for r in registros:
    lon_g, lat_g = r["geom_longitude"], r["geom_latitude"]
    lon_a, lat_a = r["longitude_atributo"], r["latitude_atributo"]
    d = distancia_haversine_m(lon_g, lat_g, lon_a, lat_a)
    r["distancia_geom_atributo_m"] = d

    if lon_g is None or lat_g is None:
        status = "SEM_GEOMETRIA"
    elif lon_a is None or lat_a is None:
        status = "SEM_ATRIBUTO"
    elif d <= LIMITE_IDENTICA_M:
        status = "IDENTICA"
    elif d <= TOLERANCIA_COORD_M:
        status = "PROXIMA"
    else:
        # verificação informativa de possível inversão lon/lat no atributo
        d_inv = distancia_haversine_m(lon_g, lat_g, lat_a, lon_a)
        status = "DIVERGENTE_POSSIVEL_INVERSAO" if d_inv is not None and d_inv <= TOLERANCIA_COORD_M else "DIVERGENTE"
    r["status_coordenada"] = status

    r["geometria_fora_bbox"] = None if lon_g is None or lat_g is None else not (
        BBOX_MSP["lon_min"] <= lon_g <= BBOX_MSP["lon_max"] and BBOX_MSP["lat_min"] <= lat_g <= BBOX_MSP["lat_max"]
    )

df_coord = pd.DataFrame([{
    "cnes": r["cnes"], "placemark_indice": r["placemark_indice"],
    "geom_longitude": r["geom_longitude"], "geom_latitude": r["geom_latitude"],
    "longitude_atributo": r["longitude_atributo"], "latitude_atributo": r["latitude_atributo"],
    "distancia_geom_atributo_m": r["distancia_geom_atributo_m"], "status_coordenada": r["status_coordenada"],
    "geometria_fora_bbox": r["geometria_fora_bbox"],
} for r in registros])

# Resumo por status (sempre há linhas: 485 registros)
df_resumo_coord = (df_coord.groupby("status_coordenada", dropna=False)
                   .agg(qtd=("cnes", "size"),
                        dist_min_m=("distancia_geom_atributo_m", "min"),
                        dist_max_m=("distancia_geom_atributo_m", "max"))
                   .reset_index().round(3))
display(df_resumo_coord)
print("Distribuição da distância geometria x atributo (m):")
print(df_coord.distancia_geom_atributo_m.describe(percentiles=[0.5, 0.9, 0.99]).round(3).to_string())

# Registros que não são IDENTICA ou estão fora do envelope (exibidos só se existirem)
df_coord_atencao = df_coord[(df_coord.status_coordenada != "IDENTICA") | (df_coord.geometria_fora_bbox == True)]
if df_coord_atencao.empty:
    print("Todos os pontos são IDENTICA e estão dentro do envelope do MSP.")
else:
    print(f"Pontos que exigem atenção: {len(df_coord_atencao)} (nenhuma coordenada foi corrigida)")
    display(df_coord_atencao.sort_values("distancia_geom_atributo_m", ascending=False))

# COMMAND ----------

# DBTITLE 1,6. Regras de validação (bloqueantes, alertas e vazios)
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

cnes_validos = [r["cnes"] for r in registros if r["cnes"]]
duplicados = sorted({c for c in cnes_validos if cnes_validos.count(c) > 1})

# Bloqueantes: impedem a gravação
registrar("QTD_REGISTROS", f"Quantidade de registros deve ser {QTD_ESPERADA} (encontrados {len(registros)})",
          "BLOQUEANTE", [] if len(registros) == QTD_ESPERADA else [f"encontrados={len(registros)}"])
registrar("GEOMETRIA_PONTO_UNICO", "Cada placemark deve ter exatamente 1 Point e nenhum Polygon", "BLOQUEANTE",
          [identificador(r) for r in registros if r["qtd_pontos"] != 1 or r["qtd_poligonos"] != 0])
registrar("CNES_VALIDO", "CNES presente e com 1 a 7 dígitos", "BLOQUEANTE",
          [identificador(r) for r in registros if r["cnes"] is None])
registrar("CNES_UNICO", "CNES não pode se repetir", "BLOQUEANTE", duplicados)
registrar("GEOMETRIA_VALIDA", "Coordenadas da geometria convertíveis em número", "BLOQUEANTE",
          [identificador(r) for r in registros if r["geom_longitude"] is None or r["geom_latitude"] is None])

# Alertas: registrados, não impedem a gravação
registrar("CNES_COMPLETADO_ZEROS", "CNES com menos de 7 dígitos completado com zeros", "ALERTA",
          [identificador(r) for r in registros if r["status_cnes"] == "COMPLETADO_ZEROS"])
registrar("CHAVES_AUSENTES", "Registro sem alguma chave esperada na descrição HTML", "ALERTA",
          [identificador(r) for r in registros if r["chaves_ausentes"]])
registrar("CHAVES_EXTRAS", "Chaves não mapeadas (preservadas em atributos_extras_json)", "ALERTA",
          [identificador(r) for r in registros if r["atributos_extras_json"]])
registrar("CHAVES_DUPLICADAS", "Chave repetida na mesma descrição (mantido o último valor)", "ALERTA",
          [identificador(r) for r in registros if r["chaves_duplicadas"]])
registrar("COORD_ATRIBUTO_NAO_CONVERTIDA", "LONSIRGAS/LATSIRGAS ausente ou não conversível", "ALERTA",
          [identificador(r) for r in registros if r["status_conversao_coord_atributo"] != "OK"])
registrar("COORD_DIVERGENTE", f"Distância geometria x atributo acima de {TOLERANCIA_COORD_M} m", "ALERTA",
          [identificador(r) for r in registros if r["status_coordenada"].startswith("DIVERGENTE")])
registrar("GEOMETRIA_FORA_BBOX", "Ponto fora do envelope aproximado do MSP", "ALERTA",
          [identificador(r) for r in registros if r["geometria_fora_bbox"]])
registrar("ALTITUDE_NAO_ZERO", "Altitude da geometria diferente de 0", "ALERTA",
          [identificador(r) for r in registros if r["geom_altitude"] not in (None, 0.0)])
registrar("CEP_FORMATO", "CEP fora do padrão 00000-000", "ALERTA",
          [identificador(r) for r in registros if r["cep_formato_valido"] is False])
registrar("NOME_PLACEMARK_DIFERE_DIVULGACAO", "Nome do placemark diferente de Nome_Divul (informativo)", "ALERTA",
          [identificador(r) for r in registros if r["nome_placemark"] != r["nome_divulgacao"]])

# Informativas: quantidade de valores vazios (NULL) por coluna de origem
for coluna in CAMPOS_ORIGEM.values():
    registrar(f"VAZIO_{coluna.upper()}", f"Valores vazios em {coluna} (mantidos como NULL)", "INFO",
              [identificador(r) for r in registros if r[coluna] is None])

# Resultado das validações (exibido no notebook; não é gravado em tabela separada)
df_regras = pd.DataFrame(regras)
display(df_regras[["regra", "severidade", "qtd_afetados", "aprovado", "descricao"]])

# Detalhe dos registros afetados por bloqueios e alertas (exibido só se houver)
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
# IF NOT EXISTS: se a tabela já existir, nada é alterado.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABELA_CADASTRO} (
    cnes                          STRING  NOT NULL COMMENT 'CNES normalizado (texto, 7 dígitos) - chave de integração',
    cnes_original                 STRING  COMMENT 'CNES como consta na descrição HTML',
    status_cnes                   STRING  COMMENT 'OK ou COMPLETADO_ZEROS',
    nome_placemark                STRING  COMMENT 'Nome do placemark no KML (descritivo; não é chave)',
    nome_divulgacao               STRING  COMMENT 'Nome_Divul (descritivo; não é chave)',
    telefone_1                    STRING  COMMENT 'Fone_1',
    telefone_2                    STRING  COMMENT 'Fone_2',
    logradouro                    STRING  COMMENT 'Logradouro',
    numero_logradouro             STRING  COMMENT 'Numero_Log (texto; pode conter valores não numéricos)',
    complemento                   STRING  COMMENT 'Complement',
    bairro                        STRING  COMMENT 'Bairro',
    cep                           STRING  COMMENT 'CEP como na origem',
    cep_formato_valido            BOOLEAN COMMENT 'CEP no padrão 00000-000',
    observacao_endereco           STRING  COMMENT 'Obs_Endere',
    horario_funcionamento         STRING  COMMENT 'Horario_Fu',
    subprefeitura                 STRING  COMMENT 'SubPref_',
    supervisao_tecnica_saude      STRING  COMMENT 'STS',
    uvis                          STRING  COMMENT 'UVIS',
    coordenadoria_regional_saude  STRING  COMMENT 'CRS (Coordenadoria Regional de Saúde)',
    geom_longitude                DOUBLE  COMMENT 'Longitude da geometria Point do KML',
    geom_latitude                 DOUBLE  COMMENT 'Latitude da geometria Point do KML',
    geom_altitude                 DOUBLE  COMMENT 'Altitude da geometria Point do KML',
    geometria_wkt                 STRING  COMMENT 'Geometria em WKT (lon lat), texto original das coordenadas',
    crs_geometria                 STRING  COMMENT 'Referência da geometria (EPSG:4326, padrão KML)',
    longitude_atributo_texto      STRING  COMMENT 'LONSIRGAS como na origem',
    latitude_atributo_texto       STRING  COMMENT 'LATSIRGAS como na origem',
    longitude_atributo            DOUBLE  COMMENT 'LONSIRGAS convertido',
    latitude_atributo             DOUBLE  COMMENT 'LATSIRGAS convertido',
    status_conversao_coord_atributo STRING COMMENT 'Resultado da conversão de LONSIRGAS/LATSIRGAS',
    crs_atributos                 STRING  COMMENT 'Referência dos atributos (EPSG:4674, SIRGAS 2000)',
    distancia_geom_atributo_m     DOUBLE  COMMENT 'Distância (haversine) entre geometria e atributos, em metros',
    status_coordenada             STRING  COMMENT 'IDENTICA, PROXIMA, DIVERGENTE, DIVERGENTE_POSSIVEL_INVERSAO, SEM_GEOMETRIA ou SEM_ATRIBUTO',
    geometria_fora_bbox           BOOLEAN COMMENT 'Ponto fora do envelope aproximado do MSP',
    atributos_extras_json         STRING  COMMENT 'Chaves da descrição não mapeadas (JSON)',
    chaves_ausentes               ARRAY<STRING> COMMENT 'Chaves esperadas ausentes na descrição',
    chaves_duplicadas             ARRAY<STRING> COMMENT 'Chaves repetidas na descrição',
    placemark_indice              INT     COMMENT 'Posição do placemark na pasta do KML',
    titulo_descricao              STRING  COMMENT 'Primeiro segmento da descrição HTML',
    style_url                     STRING  COMMENT 'styleUrl do placemark',
    coordenadas_texto             STRING  COMMENT 'Texto original de <coordinates>',
    descricao_html_original       STRING  COMMENT 'Descrição HTML original (rastreabilidade)',
    kml_pasta                     STRING  COMMENT 'Pasta do KML de origem',
    fonte_arquivo                 STRING  COMMENT 'Caminho do arquivo Bronze',
    fonte_sha256                  STRING  NOT NULL COMMENT 'SHA-256 do arquivo Bronze processado',
    id_ingestao_bronze            STRING  COMMENT 'id_ingestao do manifesto Bronze',
    id_execucao                   STRING  COMMENT 'Identificador desta execução do notebook 02',
    data_processamento            TIMESTAMP COMMENT 'Data/hora do processamento (UTC)'
)
USING DELTA
COMMENT 'Projeto Saúde Perto SP - Silver: cadastro das UBS (pontos do KML Consulta UBS). Chave: cnes.'
""")
print("Tabela disponível:", TABELA_CADASTRO)

# COMMAND ----------

# DBTITLE 1,8. Gravação idempotente do cadastro
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, BooleanType,
                               IntegerType, TimestampType, ArrayType)

# Esquema na mesma ordem e com os mesmos tipos da tabela Delta
colunas_cadastro = [
    ("cnes", StringType(), False), ("cnes_original", StringType(), True), ("status_cnes", StringType(), True),
    ("nome_placemark", StringType(), True), ("nome_divulgacao", StringType(), True),
    ("telefone_1", StringType(), True), ("telefone_2", StringType(), True),
    ("logradouro", StringType(), True), ("numero_logradouro", StringType(), True), ("complemento", StringType(), True),
    ("bairro", StringType(), True), ("cep", StringType(), True), ("cep_formato_valido", BooleanType(), True),
    ("observacao_endereco", StringType(), True), ("horario_funcionamento", StringType(), True),
    ("subprefeitura", StringType(), True), ("supervisao_tecnica_saude", StringType(), True),
    ("uvis", StringType(), True), ("coordenadoria_regional_saude", StringType(), True),
    ("geom_longitude", DoubleType(), True), ("geom_latitude", DoubleType(), True), ("geom_altitude", DoubleType(), True),
    ("geometria_wkt", StringType(), True), ("crs_geometria", StringType(), True),
    ("longitude_atributo_texto", StringType(), True), ("latitude_atributo_texto", StringType(), True),
    ("longitude_atributo", DoubleType(), True), ("latitude_atributo", DoubleType(), True),
    ("status_conversao_coord_atributo", StringType(), True), ("crs_atributos", StringType(), True),
    ("distancia_geom_atributo_m", DoubleType(), True), ("status_coordenada", StringType(), True),
    ("geometria_fora_bbox", BooleanType(), True), ("atributos_extras_json", StringType(), True),
    ("chaves_ausentes", ArrayType(StringType()), True), ("chaves_duplicadas", ArrayType(StringType()), True),
    ("placemark_indice", IntegerType(), True), ("titulo_descricao", StringType(), True), ("style_url", StringType(), True),
    ("coordenadas_texto", StringType(), True), ("descricao_html_original", StringType(), True),
    ("kml_pasta", StringType(), True), ("fonte_arquivo", StringType(), True), ("fonte_sha256", StringType(), False),
    ("id_ingestao_bronze", StringType(), True), ("id_execucao", StringType(), True), ("data_processamento", TimestampType(), True),
]
schema_cadastro = StructType([StructField(n, t, nul) for n, t, nul in colunas_cadastro])

rastreio = {"kml_pasta": PASTA_PONTOS, "fonte_arquivo": caminho_kml, "fonte_sha256": sha_atual,
            "id_ingestao_bronze": id_ingestao_bronze, "id_execucao": id_execucao, "data_processamento": data_execucao}
linhas = [tuple({**r, **rastreio}[n] for n, _, _ in colunas_cadastro) for r in registros]

# Conferir que o esquema do DataFrame coincide com o da tabela existente (evita gravação incompatível)
colunas_tabela = [c.name for c in spark.table(TABELA_CADASTRO).schema]
assert colunas_tabela == [n for n, _, _ in colunas_cadastro], "Esquema da tabela difere do esperado; gravação interrompida"

# Idempotência baseada no SHA-256 da fonte
estado = spark.sql(
    f"SELECT count(*) AS n, collect_set(fonte_sha256) AS versoes FROM {TABELA_CADASTRO}"
).first()

if estado["n"] == 0:
    spark.createDataFrame(linhas, schema=schema_cadastro).write.mode("append").saveAsTable(TABELA_CADASTRO)
    status_gravacao = "GRAVADO"
elif list(estado["versoes"]) == [sha_atual] and estado["n"] == len(linhas):
    status_gravacao = "JA_PROCESSADO"
else:
    raise RuntimeError(
        f"{TABELA_CADASTRO} já contém {estado['n']} linha(s) de versão(ões) {list(estado['versoes'])}, "
        f"diferente do processamento atual ({len(linhas)} linhas, SHA-256 {sha_atual}). "
        "Nada foi gravado; reprocessamento exige decisão explícita."
    )

print(f"Cadastro: {status_gravacao} | linhas preparadas: {len(linhas)} | id_execucao = {id_execucao}")

# COMMAND ----------

# DBTITLE 1,9. Validação pós-gravação
# Conferência independente, lendo a tabela gravada
resumo = spark.sql(f"""
    SELECT count(*)                                   AS linhas,
           count(DISTINCT cnes)                       AS cnes_distintos,
           sum(CASE WHEN cnes RLIKE '^[0-9]{{7}}$' THEN 1 ELSE 0 END) AS cnes_7_digitos,
           count(DISTINCT fonte_sha256)               AS versoes_fonte,
           max(fonte_sha256)                          AS fonte_sha256
    FROM {TABELA_CADASTRO}
""").first()
print(resumo.asDict())

assert resumo["linhas"] == QTD_ESPERADA, f"Esperadas {QTD_ESPERADA} linhas, encontradas {resumo['linhas']}"
assert resumo["cnes_distintos"] == resumo["linhas"], "CNES duplicado na tabela gravada"
assert resumo["cnes_7_digitos"] == resumo["linhas"], "Existe CNES fora do padrão de 7 dígitos"
assert resumo["versoes_fonte"] == 1 and resumo["fonte_sha256"] == sha_atual, "Versão da fonte na tabela difere da Bronze"

display(spark.sql(f"""
    SELECT status_coordenada, count(*) AS qtd,
           round(min(distancia_geom_atributo_m), 3) AS dist_min_m,
           round(max(distancia_geom_atributo_m), 3) AS dist_max_m
    FROM {TABELA_CADASTRO} GROUP BY status_coordenada ORDER BY qtd DESC
"""))

display(spark.sql(f"""
    SELECT cnes, nome_divulgacao, logradouro, numero_logradouro, bairro, cep, telefone_1,
           geom_longitude, geom_latitude, longitude_atributo, latitude_atributo, status_coordenada
    FROM {TABELA_CADASTRO} ORDER BY cnes LIMIT 5
"""))

# Inconsistências preservadas nas colunas de status da tabela gravada
display(spark.sql(f"""
    SELECT 'status_cnes <> OK'                    AS verificacao, count_if(status_cnes <> 'OK') AS qtd FROM {TABELA_CADASTRO}
    UNION ALL SELECT 'status_conversao_coord_atributo <> OK', count_if(status_conversao_coord_atributo <> 'OK') FROM {TABELA_CADASTRO}
    UNION ALL SELECT 'status_coordenada = PROXIMA', count_if(status_coordenada = 'PROXIMA') FROM {TABELA_CADASTRO}
    UNION ALL SELECT 'status_coordenada DIVERGENTE*', count_if(status_coordenada LIKE 'DIVERGENTE%') FROM {TABELA_CADASTRO}
    UNION ALL SELECT 'geometria_fora_bbox', count_if(geometria_fora_bbox) FROM {TABELA_CADASTRO}
    UNION ALL SELECT 'cep_formato_valido = false', count_if(NOT cep_formato_valido) FROM {TABELA_CADASTRO}
    UNION ALL SELECT 'chaves_ausentes não vazio', count_if(size(chaves_ausentes) > 0) FROM {TABELA_CADASTRO}
    UNION ALL SELECT 'chaves_duplicadas não vazio', count_if(size(chaves_duplicadas) > 0) FROM {TABELA_CADASTRO}
"""))
print("Silver cadastro UBS: APROVADA")

# COMMAND ----------

# DBTITLE 1,Resumo e próximos passos
# MAGIC %md
# MAGIC ## Resultado esperado
# MAGIC * 485 linhas em `ssp_silver_ubs_cadastro`, 485 CNES distintos com 7 dígitos e uma única versão de fonte (SHA-256 da Bronze).
# MAGIC * Regras bloqueantes aprovadas; alertas e contagens de vazios exibidos no notebook; inconsistências por registro preservadas nas colunas de status.
# MAGIC * Reexecução com a mesma versão da fonte: `status_gravacao = JA_PROCESSADO`, sem nova gravação.
# MAGIC
# MAGIC ## Próximo notebook
# MAGIC `03_silver_areas_abrangencia`: geometrias oficiais do KMZ, comparação com os polígonos do KML e validação espacial ponto x polígono por CNES (`covers`).