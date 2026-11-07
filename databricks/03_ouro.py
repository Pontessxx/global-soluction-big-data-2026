# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ouro — modelo dimensional + LGPD (requisitos 07 e 08)
# MAGIC ```
# MAGIC                 dim_tempo ──┐
# MAGIC               dim_distrito ─┤
# MAGIC                dim_servico ─┼── fato_pedido (1 linha por pedido do SP156)
# MAGIC                  dim_canal ─┤
# MAGIC                  dim_clima ─┘   (chuva do dia da abertura)
# MAGIC ```
# MAGIC * `dim_distrito` traz o **denominador**: nº de pontos de iluminação por distrito (GeoSampa).
# MAGIC * **LGPD na Ouro:** a fato **não guarda** logradouro, número, CEP completo nem coordenada exata. Guarda hash do endereço
# MAGIC   (com sal), CEP mascarado e coordenada arredondada (~110 m). O SP156 aberto não traz nome, CPF nem telefone — a máscara
# MAGIC   cobre o endereço do local do problema, que pode identificar uma residência.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC USE SCHEMA rua_acesa;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Ponto → distrito (join espacial)
# MAGIC Feito com `shapely` (STRtree) no driver: 663 mil pontos × 96 polígonos cabem na memória. Pontos fora de qualquer
# MAGIC polígono (borda do município) ficam sem distrito e são contados.

# COMMAND ----------

# MAGIC %pip install shapely

# COMMAND ----------

import numpy as np
import pandas as pd
from shapely import STRtree, points, wkt

dist = spark.table("prata_distritos").select("cd_distrito", "wkt").toPandas()
geoms = [wkt.loads(w) for w in dist.wkt]
arvore = STRtree(geoms)

pts = spark.table("prata_ilum_pontos").select("id_ponto", "lon", "lat").toPandas()
res = arvore.query(points(pts.lon.to_numpy(), pts.lat.to_numpy()), predicate="within")

mapa = pd.DataFrame({"id_ponto": pts.id_ponto.to_numpy()[res[0]],
                     "cd_distrito": dist.cd_distrito.to_numpy()[res[1]]}
                    ).drop_duplicates("id_ponto")   # ponto na divisa de 2 distritos: fica com 1
(spark.createDataFrame(mapa).write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable("ouro_ponto_distrito"))
print(f"pontos: {len(pts):,} | com distrito: {len(mapa):,} | sem distrito: {len(pts) - len(mapa):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Dimensões

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_tempo AS
# MAGIC SELECT cast(date_format(d, 'yyyyMMdd') AS INT) AS sk_tempo, d AS dt,
# MAGIC        year(d) AS ano, quarter(d) AS trimestre, month(d) AS mes,
# MAGIC        date_format(d, 'EEEE') AS dia_semana, dayofweek(d) AS dia_semana_num
# MAGIC FROM (SELECT explode(sequence((SELECT min(dt_abertura) FROM prata_sp156), current_date(), interval 1 day)) AS d);
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_distrito AS
# MAGIC SELECT d.cd_distrito AS sk_distrito, d.nm_distrito, d.sg_distrito, d.cd_subprefeitura,
# MAGIC        coalesce(p.qt_pontos, 0) AS qt_pontos_iluminacao,
# MAGIC        coalesce(p.qt_led, 0)    AS qt_pontos_led,
# MAGIC        round(100 * coalesce(p.qt_led, 0) / nullif(p.qt_pontos, 0), 1) AS pct_led
# MAGIC FROM prata_distritos d
# MAGIC LEFT JOIN (SELECT m.cd_distrito, count(*) AS qt_pontos, count_if(i.tipo_lampada = 'LED') AS qt_led
# MAGIC            FROM ouro_ponto_distrito m JOIN prata_ilum_pontos i USING (id_ponto) GROUP BY m.cd_distrito) p
# MAGIC   ON p.cd_distrito = d.cd_distrito;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_servico AS
# MAGIC SELECT DISTINCT abs(xxhash64(servico)) AS sk_servico, tema, assunto, servico FROM prata_sp156;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_canal AS
# MAGIC SELECT DISTINCT abs(xxhash64(canal)) AS sk_canal, canal FROM prata_sp156 WHERE canal IS NOT NULL;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_clima AS
# MAGIC SELECT cast(date_format(dt, 'yyyyMMdd') AS INT) AS sk_tempo, dt, precipitacao_mm, horas_chuva,
# MAGIC        CASE WHEN precipitacao_mm >= 10 THEN 'CHUVA FORTE' WHEN precipitacao_mm >= 1 THEN 'CHUVA'
# MAGIC             ELSE 'SEM CHUVA' END AS faixa_chuva
# MAGIC FROM prata_clima;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Fato com LGPD
# MAGIC O **sal** do hash é digitado no widget abaixo e **não é salvo** em lugar nenhum (nem no Git). Guarde-o fora do repositório.

# COMMAND ----------

dbutils.widgets.text("salt", "", "Sal do hash (não commitar)")
SALT = dbutils.widgets.get("salt")
if not SALT:
    raise ValueError("Preencha o widget 'salt' com um texto secreto antes de rodar a fato.")

spark.sql(f"""
CREATE OR REPLACE TABLE fato_pedido AS
SELECT
  p.chave                                             AS sk_pedido,
  cast(date_format(p.dt_abertura, 'yyyyMMdd') AS INT) AS sk_tempo_abertura,
  cast(date_format(p.dt_finalizacao, 'yyyyMMdd') AS INT) AS sk_tempo_finalizacao,
  dd.sk_distrito,
  abs(xxhash64(p.servico))                            AS sk_servico,
  abs(xxhash64(p.canal))                              AS sk_canal,
  p.status,
  p.status IN ('ABERTO', 'EM ANDAMENTO')              AS em_aberto,
  p.dt_abertura,
  p.dt_finalizacao,
  p.dias_atendimento,
  -- dias em aberto até a data do retrato do SP156 (maior data de finalização observada)
  CASE WHEN p.status IN ('ABERTO', 'EM ANDAMENTO')
       THEN datediff((SELECT max(dt_finalizacao) FROM prata_sp156), p.dt_abertura) END AS dias_em_aberto,
  -- ===== LGPD: nada de endereço exato ou coordenada exata na Ouro =====
  sha2(concat_ws('|', p.logradouro, p.numero, p.cep, '{SALT}'), 256)  AS endereco_hash,
  concat(substr(p.cep, 1, 5), '-***')                                  AS cep_mascarado,
  round(p.lat, 3)                                                      AS lat_aprox,
  round(p.lon, 3)                                                      AS lon_aprox
FROM prata_sp156 p
LEFT JOIN dim_distrito dd ON dd.nm_distrito = p.distrito
""")
spark.sql("OPTIMIZE fato_pedido")
print(spark.sql("SELECT count(*) c, count_if(sk_distrito IS NULL) sem_distrito FROM fato_pedido").first())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Tabela que o MVP consome: `ouro_mvp_ranking_distritos`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ouro_mvp_ranking_distritos AS
# MAGIC SELECT d.nm_distrito AS distrito,
# MAGIC        count(*)                                             AS pedidos,
# MAGIC        count_if(f.em_aberto)                                AS abertos,
# MAGIC        round(100 * count_if(f.em_aberto) / count(*), 1)     AS pct_abertos,
# MAGIC        count_if(f.em_aberto AND f.dias_em_aberto > 30)      AS abertos_mais_30d,
# MAGIC        percentile_approx(CASE WHEN f.status = 'FINALIZADA' THEN f.dias_atendimento END, 0.5) AS mediana_dias_finalizados,
# MAGIC        count_if(f.status = 'FINALIZADA')                    AS finalizados,
# MAGIC        d.qt_pontos_iluminacao                               AS pontos,
# MAGIC        d.pct_led,
# MAGIC        round(1000 * count(*) / nullif(d.qt_pontos_iluminacao, 0), 2) AS pedidos_por_mil_pontos
# MAGIC FROM fato_pedido f JOIN dim_distrito d USING (sk_distrito)
# MAGIC GROUP BY d.nm_distrito, d.qt_pontos_iluminacao, d.pct_led;
# MAGIC
# MAGIC SELECT * FROM ouro_mvp_ranking_distritos ORDER BY pedidos DESC LIMIT 10;
