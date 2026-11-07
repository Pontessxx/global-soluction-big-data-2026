# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata — tipos, qualidade e MERGE (requisito 06)
# MAGIC Para cada fonte: (1) tipar e limpar a Bronze, (2) pegar a **versão mais recente** de cada registro,
# MAGIC (3) aplicar **regras de qualidade** separando válidos de rejeitados **com a contagem por regra**,
# MAGIC (4) `MERGE` dos válidos pela chave de negócio. Rodar de novo não duplica.
# MAGIC
# MAGIC **Chave do SP156:** o arquivo aberto **não traz número de protocolo**. A chave é um hash de
# MAGIC (data de abertura, serviço, logradouro, número, CEP, lat, lon, canal) + um ordinal para pedidos idênticos
# MAGIC (existem duplicatas reais no dado). `status`, `órgão` e `data de finalização` mudam entre coletas → o MERGE atualiza.
# MAGIC
# MAGIC *Datas:* o DAG já normaliza para `AAAA-MM-DD` (o 2º e o 3º tri/2026 usam formatos diferentes na origem).

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC USE SCHEMA rua_acesa;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prata_qualidade (
# MAGIC   executado_em TIMESTAMP, tabela STRING, regra STRING, descricao STRING,
# MAGIC   total_avaliado BIGINT, barrados BIGINT);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 SP156 — pedidos de poste em mau estado e fiação solta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_sp156_tipado AS
# MAGIC SELECT
# MAGIC   try_cast(nullif(trim(data_abertura), '') AS DATE)       AS dt_abertura,
# MAGIC   try_cast(nullif(trim(data_finalizacao), '') AS DATE)    AS dt_finalizacao,
# MAGIC   upper(nullif(trim(meio_comunicacao), ''))               AS canal,
# MAGIC   nullif(trim(tema), '')                                  AS tema,
# MAGIC   nullif(trim(assunto), '')                               AS assunto,
# MAGIC   nullif(trim(servico), '')                               AS servico,
# MAGIC   upper(nullif(trim(status), ''))                         AS status,
# MAGIC   nullif(trim(orgao), '')                                 AS orgao,
# MAGIC   upper(nullif(trim(logradouro), ''))                     AS logradouro,
# MAGIC   nullif(trim(numero), '')                                AS numero,
# MAGIC   nullif(regexp_replace(cep, '[^0-9]', ''), '')           AS cep,
# MAGIC   try_cast(nullif(trim(latitude), '') AS DOUBLE)          AS lat,
# MAGIC   try_cast(nullif(trim(longitude), '') AS DOUBLE)         AS lon,
# MAGIC   upper(nullif(trim(subprefeitura), ''))                  AS subprefeitura,
# MAGIC   upper(nullif(trim(distrito), ''))                       AS distrito,
# MAGIC   _arquivo_origem, _carregado_em
# MAGIC FROM bronze_sp156;
# MAGIC
# MAGIC -- chave base + ordinal (por arquivo) para pedidos idênticos
# MAGIC CREATE OR REPLACE TEMP VIEW v_sp156_chaveado AS
# MAGIC SELECT *,
# MAGIC   row_number() OVER (PARTITION BY _arquivo_origem, chave_base
# MAGIC                      ORDER BY dt_finalizacao NULLS LAST, status, orgao) AS ordinal
# MAGIC FROM (
# MAGIC   SELECT *, sha2(concat_ws('|', cast(dt_abertura AS STRING), servico, logradouro, numero, cep,
# MAGIC                            cast(lat AS STRING), cast(lon AS STRING), canal), 256) AS chave_base
# MAGIC   FROM v_sp156_tipado);
# MAGIC
# MAGIC -- versão mais recente de cada pedido (último arquivo carregado vence)
# MAGIC CREATE OR REPLACE TEMP VIEW v_sp156_atual AS
# MAGIC SELECT concat(chave_base, '-', ordinal) AS chave, * EXCEPT (ordinal)
# MAGIC FROM v_sp156_chaveado
# MAGIC QUALIFY row_number() OVER (PARTITION BY chave_base, ordinal
# MAGIC                            ORDER BY _carregado_em DESC, _arquivo_origem DESC) = 1;
# MAGIC
# MAGIC -- regras de qualidade: TRUE = viola a regra
# MAGIC CREATE OR REPLACE TEMP VIEW v_sp156_regras AS
# MAGIC SELECT *,
# MAGIC   (dt_abertura IS NULL OR servico IS NULL OR distrito IS NULL)                            AS r1_obrigatorio_nulo,
# MAGIC   (dt_finalizacao IS NOT NULL AND dt_abertura IS NOT NULL AND dt_finalizacao < dt_abertura) AS r2_finalizacao_antes_abertura,
# MAGIC   (lat IS NULL OR lon IS NULL
# MAGIC     OR lat NOT BETWEEN -24.01 AND -23.35 OR lon NOT BETWEEN -46.83 AND -46.36)             AS r3_coordenada_invalida,
# MAGIC   (servico IS NOT NULL AND servico NOT IN ('Denunciar Poste em mau estado',
# MAGIC                                            'Denunciar Fiação solta na calçada'))          AS r4_servico_fora_do_escopo,
# MAGIC   (status IS NULL OR status NOT IN ('ABERTO','EM ANDAMENTO','FINALIZADA','CANCELADA','INDEFERIDO')
# MAGIC     OR dt_abertura > current_date())                                                      AS r5_status_ou_data_invalidos
# MAGIC FROM v_sp156_atual;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Contagem de barrados por regra (uma linha pode violar mais de uma regra)
# MAGIC CREATE OR REPLACE TEMP VIEW v_sp156_contagem AS
# MAGIC SELECT 'prata_sp156' AS tabela, 'R1' AS regra, 'Campos obrigatórios nulos (abertura, serviço, distrito)' AS descricao, COUNT(*) AS total_avaliado, count_if(r1_obrigatorio_nulo) AS barrados FROM v_sp156_regras
# MAGIC UNION ALL SELECT 'prata_sp156','R2','Data de finalização anterior à abertura', COUNT(*), count_if(r2_finalizacao_antes_abertura) FROM v_sp156_regras
# MAGIC UNION ALL SELECT 'prata_sp156','R3','Coordenada ausente ou fora da caixa do município de SP', COUNT(*), count_if(r3_coordenada_invalida) FROM v_sp156_regras
# MAGIC UNION ALL SELECT 'prata_sp156','R4','Serviço fora do escopo do produto', COUNT(*), count_if(r4_servico_fora_do_escopo) FROM v_sp156_regras
# MAGIC UNION ALL SELECT 'prata_sp156','R5','Status inválido ou data de abertura no futuro', COUNT(*), count_if(r5_status_ou_data_invalidos) FROM v_sp156_regras;
# MAGIC
# MAGIC INSERT INTO prata_qualidade SELECT current_timestamp(), * FROM v_sp156_contagem;
# MAGIC SELECT * FROM v_sp156_contagem;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rejeitados (reconstruída a cada execução a partir de TODA a Bronze: idempotente)
# MAGIC CREATE OR REPLACE TABLE prata_sp156_rejeitados AS
# MAGIC SELECT chave, dt_abertura, servico, distrito, lat, lon, status,
# MAGIC        concat_ws(',',
# MAGIC          CASE WHEN r1_obrigatorio_nulo THEN 'R1' END, CASE WHEN r2_finalizacao_antes_abertura THEN 'R2' END,
# MAGIC          CASE WHEN r3_coordenada_invalida THEN 'R3' END, CASE WHEN r4_servico_fora_do_escopo THEN 'R4' END,
# MAGIC          CASE WHEN r5_status_ou_data_invalidos THEN 'R5' END) AS regras_violadas,
# MAGIC        _arquivo_origem
# MAGIC FROM v_sp156_regras
# MAGIC WHERE r1_obrigatorio_nulo OR r2_finalizacao_antes_abertura OR r3_coordenada_invalida
# MAGIC    OR r4_servico_fora_do_escopo OR r5_status_ou_data_invalidos;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW v_sp156_validos AS
# MAGIC SELECT chave, dt_abertura, dt_finalizacao,
# MAGIC        datediff(dt_finalizacao, dt_abertura) AS dias_atendimento,
# MAGIC        canal, tema, assunto, servico, status, orgao, logradouro, numero, cep, lat, lon,
# MAGIC        subprefeitura, distrito, _arquivo_origem, current_timestamp() AS _atualizado_em
# MAGIC FROM v_sp156_regras
# MAGIC WHERE NOT (r1_obrigatorio_nulo OR r2_finalizacao_antes_abertura OR r3_coordenada_invalida
# MAGIC            OR r4_servico_fora_do_escopo OR r5_status_ou_data_invalidos);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prata_sp156 AS SELECT * FROM v_sp156_validos WHERE 1 = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS linhas_antes FROM prata_sp156;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO prata_sp156 AS t
# MAGIC USING v_sp156_validos AS s
# MAGIC ON t.chave = s.chave
# MAGIC WHEN MATCHED AND (NOT (t.status <=> s.status) OR NOT (t.dt_finalizacao <=> s.dt_finalizacao)
# MAGIC                   OR NOT (t.orgao <=> s.orgao)) THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS linhas_depois, count(DISTINCT chave) AS chaves_distintas FROM prata_sp156;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 GeoSampa — pontos de iluminação pública

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_pontos_atual AS
# MAGIC SELECT * EXCEPT (rn) FROM (
# MAGIC   SELECT try_cast(cd_identificador_iluminacao_publica AS BIGINT)            AS id_ponto,
# MAGIC          upper(nullif(trim(dc_tipo_iluminacao_publica), ''))                AS tipo_lampada,
# MAGIC          upper(nullif(trim(tx_endereco), ''))                               AS endereco,
# MAGIC          try_cast(regexp_extract(ge_ponto, 'POINT [(]([-0-9.eE]+) ([-0-9.eE]+)[)]', 1) AS DOUBLE) AS lon,
# MAGIC          try_cast(regexp_extract(ge_ponto, 'POINT [(]([-0-9.eE]+) ([-0-9.eE]+)[)]', 2) AS DOUBLE) AS lat,
# MAGIC          _arquivo_origem, _carregado_em,
# MAGIC          row_number() OVER (PARTITION BY cd_identificador_iluminacao_publica
# MAGIC                             ORDER BY _carregado_em DESC, _arquivo_origem DESC) AS rn
# MAGIC   FROM bronze_geosampa_iluminacao) WHERE rn = 1;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW v_pontos_regras AS
# MAGIC SELECT *,
# MAGIC   (id_ponto IS NULL)                                                              AS p1_id_nulo,
# MAGIC   (lat IS NULL OR lon IS NULL OR lat NOT BETWEEN -24.01 AND -23.35
# MAGIC     OR lon NOT BETWEEN -46.83 AND -46.36)                                        AS p2_coordenada_invalida,
# MAGIC   (tipo_lampada IS NULL)                                                          AS p3_tipo_nulo
# MAGIC FROM v_pontos_atual;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW v_pontos_contagem AS
# MAGIC SELECT 'prata_ilum_pontos' AS tabela, 'P1' AS regra, 'Identificador do ponto nulo' AS descricao, COUNT(*) AS total_avaliado, count_if(p1_id_nulo) AS barrados FROM v_pontos_regras
# MAGIC UNION ALL SELECT 'prata_ilum_pontos','P2','Coordenada ausente ou fora da caixa do município', COUNT(*), count_if(p2_coordenada_invalida) FROM v_pontos_regras
# MAGIC UNION ALL SELECT 'prata_ilum_pontos','P3','Tipo de lâmpada nulo', COUNT(*), count_if(p3_tipo_nulo) FROM v_pontos_regras;
# MAGIC
# MAGIC INSERT INTO prata_qualidade SELECT current_timestamp(), * FROM v_pontos_contagem;
# MAGIC SELECT * FROM v_pontos_contagem;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_pontos_validos AS
# MAGIC SELECT id_ponto, tipo_lampada, endereco, lat, lon, _arquivo_origem, current_timestamp() AS _atualizado_em
# MAGIC FROM v_pontos_regras WHERE NOT (p1_id_nulo OR p2_coordenada_invalida OR p3_tipo_nulo);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prata_ilum_pontos AS SELECT * FROM v_pontos_validos WHERE 1 = 0;
# MAGIC
# MAGIC MERGE INTO prata_ilum_pontos AS t USING v_pontos_validos AS s ON t.id_ponto = s.id_ponto
# MAGIC WHEN MATCHED AND (NOT (t.tipo_lampada <=> s.tipo_lampada) OR NOT (t.lat <=> s.lat) OR NOT (t.lon <=> s.lon))
# MAGIC   THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC SELECT COUNT(*) AS pontos_na_prata, count(DISTINCT id_ponto) AS ids_distintos FROM prata_ilum_pontos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 GeoSampa — distritos (polígonos em WKT)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_distritos_validos AS
# MAGIC SELECT * EXCEPT (rn) FROM (
# MAGIC   SELECT try_cast(cd_identificador_distrito AS BIGINT)        AS cd_distrito,
# MAGIC          upper(nullif(trim(nm_distrito_municipal), ''))       AS nm_distrito,
# MAGIC          upper(nullif(trim(sg_distrito_municipal), ''))       AS sg_distrito,
# MAGIC          try_cast(cd_identificador_subprefeitura AS BIGINT)   AS cd_subprefeitura,
# MAGIC          ge_poligono                                          AS wkt,
# MAGIC          _arquivo_origem, current_timestamp() AS _atualizado_em,
# MAGIC          row_number() OVER (PARTITION BY cd_identificador_distrito
# MAGIC                             ORDER BY _carregado_em DESC, _arquivo_origem DESC) AS rn
# MAGIC   FROM bronze_geosampa_distritos)
# MAGIC WHERE rn = 1 AND cd_distrito IS NOT NULL AND nm_distrito IS NOT NULL
# MAGIC   AND (wkt LIKE 'POLYGON%' OR wkt LIKE 'MULTIPOLYGON%');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prata_distritos AS SELECT * FROM v_distritos_validos WHERE 1 = 0;
# MAGIC
# MAGIC MERGE INTO prata_distritos AS t USING v_distritos_validos AS s ON t.cd_distrito = s.cd_distrito
# MAGIC WHEN MATCHED AND NOT (t.wkt <=> s.wkt) THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC SELECT COUNT(*) AS distritos FROM prata_distritos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Open-Meteo — chuva diária (listas paralelas → `arrays_zip` + `explode`)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_clima_atual AS
# MAGIC SELECT * EXCEPT (rn) FROM (
# MAGIC   SELECT try_cast(z.time AS DATE) AS dt, z.precipitation_sum AS precipitacao_mm, z.precipitation_hours AS horas_chuva,
# MAGIC          _arquivo_origem, _carregado_em,
# MAGIC          row_number() OVER (PARTITION BY z.time ORDER BY _carregado_em DESC, _arquivo_origem DESC) AS rn
# MAGIC   FROM (SELECT explode(arrays_zip(daily.time, daily.precipitation_sum, daily.precipitation_hours)) AS z,
# MAGIC                _arquivo_origem, _carregado_em FROM bronze_openmeteo)) WHERE rn = 1;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW v_clima_regras AS
# MAGIC SELECT *, (dt IS NULL OR precipitacao_mm IS NULL)                  AS c1_nulo,
# MAGIC           (precipitacao_mm < 0 OR precipitacao_mm > 500 OR horas_chuva < 0 OR horas_chuva > 24) AS c2_fora_da_faixa
# MAGIC FROM v_clima_atual;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW v_clima_contagem AS
# MAGIC SELECT 'prata_clima' AS tabela, 'C1' AS regra, 'Data ou precipitação nula' AS descricao, COUNT(*) AS total_avaliado, count_if(c1_nulo) AS barrados FROM v_clima_regras
# MAGIC UNION ALL SELECT 'prata_clima','C2','Precipitação fora de 0–500 mm ou horas fora de 0–24', COUNT(*), count_if(c2_fora_da_faixa) FROM v_clima_regras;
# MAGIC
# MAGIC INSERT INTO prata_qualidade SELECT current_timestamp(), * FROM v_clima_contagem;
# MAGIC SELECT * FROM v_clima_contagem;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_clima_validos AS
# MAGIC SELECT dt, precipitacao_mm, horas_chuva, _arquivo_origem, current_timestamp() AS _atualizado_em
# MAGIC FROM v_clima_regras WHERE NOT (c1_nulo OR c2_fora_da_faixa);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prata_clima AS SELECT * FROM v_clima_validos WHERE 1 = 0;
# MAGIC
# MAGIC MERGE INTO prata_clima AS t USING v_clima_validos AS s ON t.dt = s.dt
# MAGIC WHEN MATCHED AND (NOT (t.precipitacao_mm <=> s.precipitacao_mm) OR NOT (t.horas_chuva <=> s.horas_chuva)) THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC SELECT COUNT(*) AS dias, min(dt) AS primeiro_dia, max(dt) AS ultimo_dia FROM prata_clima;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidência do requisito 06 — barrados por regra (última execução de cada tabela)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tabela, regra, descricao, total_avaliado, barrados
# MAGIC FROM prata_qualidade
# MAGIC QUALIFY executado_em = max(executado_em) OVER (PARTITION BY tabela)
# MAGIC ORDER BY tabela, regra;

# COMMAND ----------

# MAGIC %md
# MAGIC > **[NÃO VERIFICADO no Free Edition]**: `QUALIFY`, `count_if`, `EXCEPT (col)` e `try_cast` são sintaxe de Databricks SQL,
# MAGIC > mas este notebook ainda não foi executado. Se algo falhar, ajuste a célula e anote a correção no relatório.
# MAGIC >
# MAGIC > A caixa de coordenadas (lat −24,01 a −23,35; lon −46,83 a −46,36) é **aproximada** para o município de SP — não é o polígono oficial.
# MAGIC > Conferência local equivalente em Python sobre o arquivo real de 05/11/2026 (1.642 linhas do SP156): R1 = 2, R2 = 0, R3 = 2.
# MAGIC > Esses números servem só de expectativa: os valores que valem para o relatório são os que o notebook mostrar no Databricks.
