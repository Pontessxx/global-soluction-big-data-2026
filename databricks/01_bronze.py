# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — carga incremental e idempotente (requisitos 04 e 05)
# MAGIC Usa **COPY INTO**: o Delta guarda quais arquivos já foram carregados, então
# MAGIC * após o 2º lote só os arquivos **novos** entram;
# MAGIC * rodar o notebook de novo **não duplica nada**.
# MAGIC
# MAGIC Cada linha guarda `_arquivo_origem` e `_carregado_em`. A Bronze é dado bruto: tudo como texto (exceto o JSON).
# MAGIC
# MAGIC **Como gerar a evidência:** rode este notebook (a) depois do lote 1, (b) depois de subir o lote 2, (c) uma 2ª vez
# MAGIC sem subir nada. A tabela `controle_contagens` registra antes/depois de cada execução — print dela é o item 07 do relatório.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC USE SCHEMA rua_acesa;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze_sp156 (
# MAGIC   data_abertura STRING, data_finalizacao STRING, meio_comunicacao STRING, tema STRING,
# MAGIC   assunto STRING, servico STRING, status STRING, orgao STRING, logradouro STRING,
# MAGIC   numero STRING, cep STRING, latitude STRING, longitude STRING,
# MAGIC   subprefeitura STRING, distrito STRING,
# MAGIC   _arquivo_origem STRING, _carregado_em TIMESTAMP);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze_geosampa_iluminacao (
# MAGIC   fid STRING, cd_identificador_iluminacao_publica STRING, dc_tipo_iluminacao_publica STRING,
# MAGIC   ge_ponto STRING, sg_fonte_original STRING, tx_endereco STRING,
# MAGIC   _arquivo_origem STRING, _carregado_em TIMESTAMP);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze_geosampa_distritos (
# MAGIC   fid STRING, cd_identificador_distrito STRING, cd_identificador_subprefeitura STRING,
# MAGIC   ge_poligono STRING, cd_distrito_municipal STRING, nm_distrito_municipal STRING,
# MAGIC   sg_distrito_municipal STRING,
# MAGIC   _arquivo_origem STRING, _carregado_em TIMESTAMP);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze_openmeteo (
# MAGIC   latitude DOUBLE, longitude DOUBLE, timezone STRING,
# MAGIC   daily STRUCT<time: ARRAY<STRING>, precipitation_sum: ARRAY<DOUBLE>, precipitation_hours: ARRAY<DOUBLE>>,
# MAGIC   _arquivo_origem STRING, _carregado_em TIMESTAMP);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS controle_contagens (
# MAGIC   executado_em TIMESTAMP, rotulo STRING, tabela STRING,
# MAGIC   linhas_antes BIGINT, linhas_inseridas BIGINT, linhas_depois BIGINT, arquivos_na_tabela BIGINT);

# COMMAND ----------

BASE = "/Volumes/workspace/rua_acesa/chegada"
META = "_metadata.file_path AS _arquivo_origem, current_timestamp() AS _carregado_em"

# {tabela: (SELECT de origem, formato, opções, padrão de nome de arquivo)}
CARGAS = {
    "bronze_sp156": (
        f"""SELECT `Data de Abertura` AS data_abertura, `Data de Finalização` AS data_finalizacao,
                   `Meio de Comunicação` AS meio_comunicacao, Tema AS tema, Assunto AS assunto,
                   `Serviço` AS servico, Status AS status, `Órgão` AS orgao, Logradouro AS logradouro,
                   `Número` AS numero, CEP AS cep, Latitude AS latitude, Longitude AS longitude,
                   `Prefeitura Operacional` AS subprefeitura, Distrito AS distrito, {META}
            FROM '{BASE}/sp156/'""",
        "CSV", "('header'='true','inferSchema'='false','multiLine'='true','encoding'='UTF-8')", None),
    "bronze_geosampa_iluminacao": (
        f"""SELECT FID AS fid, cd_identificador_iluminacao_publica, dc_tipo_iluminacao_publica,
                   ge_ponto, sg_fonte_original, tx_endereco, {META}
            FROM '{BASE}/geosampa/' """,
        "CSV", "('header'='true','inferSchema'='false','multiLine'='true')", "iluminacao_*.csv"),
    "bronze_geosampa_distritos": (
        f"""SELECT FID AS fid, cd_identificador_distrito, cd_identificador_subprefeitura, ge_poligono,
                   cd_distrito_municipal, nm_distrito_municipal, sg_distrito_municipal, {META}
            FROM '{BASE}/geosampa/' """,
        "CSV", "('header'='true','inferSchema'='false','multiLine'='true')", "distritos_*.csv"),
    "bronze_openmeteo": (
        f"""SELECT latitude, longitude, timezone, daily, {META} FROM '{BASE}/openmeteo/'""",
        "JSON", "('multiLine'='true')", None),
}


def contar(tabela):
    return spark.sql(f"SELECT COUNT(*) c FROM {tabela}").first().c


def carregar(rotulo):
    """COPY INTO em cada tabela e registra linhas antes/depois em controle_contagens."""
    for tabela, (origem, formato, opcoes, padrao) in CARGAS.items():
        antes = contar(tabela)
        filtro = f"PATTERN = '{padrao}'" if padrao else ""
        spark.sql(f"""COPY INTO {tabela} FROM ({origem})
                      FILEFORMAT = {formato} {filtro} FORMAT_OPTIONS {opcoes}""")
        depois = contar(tabela)
        arquivos = spark.sql(f"SELECT COUNT(DISTINCT _arquivo_origem) c FROM {tabela}").first().c
        spark.sql(f"""INSERT INTO controle_contagens VALUES
                      (current_timestamp(), '{rotulo}', '{tabela}', {antes}, {depois - antes}, {depois}, {arquivos})""")
        print(f"{tabela:32s} antes={antes:>9,}  inseridas={depois - antes:>9,}  depois={depois:>9,}  arquivos={arquivos}")


# COMMAND ----------

# Troque o rótulo a cada execução: "lote1", "lote2", "lote2_reexecucao"
ROTULO = "lote1"
carregar(ROTULO)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidência — contagens antes e depois (print do relatório, item 07)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM controle_contagens ORDER BY executado_em, tabela;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidência — quais arquivos já entraram, e quantas linhas cada um trouxe

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'sp156' AS fonte, regexp_extract(_arquivo_origem, '[^/]+$', 0) AS arquivo, COUNT(*) AS linhas, MAX(_carregado_em) AS carregado_em
# MAGIC FROM bronze_sp156 GROUP BY 1, 2
# MAGIC UNION ALL
# MAGIC SELECT 'iluminacao', regexp_extract(_arquivo_origem, '[^/]+$', 0), COUNT(*), MAX(_carregado_em) FROM bronze_geosampa_iluminacao GROUP BY 1, 2
# MAGIC UNION ALL
# MAGIC SELECT 'distritos', regexp_extract(_arquivo_origem, '[^/]+$', 0), COUNT(*), MAX(_carregado_em) FROM bronze_geosampa_distritos GROUP BY 1, 2
# MAGIC UNION ALL
# MAGIC SELECT 'openmeteo', regexp_extract(_arquivo_origem, '[^/]+$', 0), COUNT(*), MAX(_carregado_em) FROM bronze_openmeteo GROUP BY 1, 2
# MAGIC ORDER BY fonte, arquivo;

# COMMAND ----------

# MAGIC %md
# MAGIC > **[NÃO VERIFICADO no Free Edition]**: a sintaxe `COPY INTO (SELECT ... FROM '<volume>')` com `_metadata` e `PATTERN`
# MAGIC > segue a documentação do Databricks, mas não foi executada. Se algum comando falhar, copie a mensagem de erro e ajuste aqui.
# MAGIC > Alternativa equivalente: **Auto Loader** (`cloudFiles`) com `trigger(availableNow=True)` e `checkpointLocation`.
