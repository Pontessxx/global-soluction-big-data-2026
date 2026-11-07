# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · As 5 consultas SQL da Ouro (requisito 07)
# MAGIC Cada consulta responde a uma pergunta do MVP. **Q2 é a consulta de impacto.**
# MAGIC Gerado a partir de `databricks/sql/consultas_ouro.sql` (mantenha os dois iguais).
# MAGIC Depois de rodar, tire print do resultado e escreva a interpretação no relatório.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC USE SCHEMA rua_acesa;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1. Quais distritos têm mais pedidos por mil pontos de iluminação? (pedidos de poste em mau estado / fiação solta, normalizados pelo tamanho da rede)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT d.nm_distrito, d.qt_pontos_iluminacao AS pontos,
# MAGIC        count(*)                                                AS pedidos,
# MAGIC        round(1000 * count(*) / d.qt_pontos_iluminacao, 2)      AS pedidos_por_mil_pontos
# MAGIC FROM fato_pedido f
# MAGIC JOIN dim_distrito d USING (sk_distrito)
# MAGIC WHERE d.qt_pontos_iluminacao > 0
# MAGIC GROUP BY d.nm_distrito, d.qt_pontos_iluminacao
# MAGIC ORDER BY pedidos_por_mil_pontos DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2 (IMPACTO). Onde a espera é maior: quantos pedidos seguem abertos há mais de 30 dias e qual a mediana dos que foram atendidos, por distrito?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT d.nm_distrito,
# MAGIC        count(*)                                                                   AS pedidos,
# MAGIC        count_if(f.em_aberto)                                                      AS abertos,
# MAGIC        count_if(f.em_aberto AND f.dias_em_aberto > 30)                            AS abertos_mais_30d,
# MAGIC        round(100 * count_if(f.em_aberto) / count(*), 1)                           AS pct_abertos,
# MAGIC        count_if(f.status = 'FINALIZADA')                                          AS finalizados,
# MAGIC        percentile_approx(CASE WHEN f.status = 'FINALIZADA' THEN f.dias_atendimento END, 0.5) AS mediana_dias_atendidos
# MAGIC FROM fato_pedido f
# MAGIC JOIN dim_distrito d USING (sk_distrito)
# MAGIC GROUP BY d.nm_distrito
# MAGIC HAVING count(*) >= 20
# MAGIC ORDER BY abertos_mais_30d DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3. O serviço (poste x fiação) muda o tempo de atendimento e a fila?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT s.servico,
# MAGIC        count(*)                                                                   AS pedidos,
# MAGIC        count_if(f.status = 'FINALIZADA')                                          AS finalizados,
# MAGIC        round(100 * count_if(f.status = 'FINALIZADA') / count(*), 1)               AS pct_finalizados,
# MAGIC        percentile_approx(CASE WHEN f.status = 'FINALIZADA' THEN f.dias_atendimento END, 0.5) AS mediana_dias,
# MAGIC        percentile_approx(CASE WHEN f.status = 'FINALIZADA' THEN f.dias_atendimento END, 0.9) AS p90_dias
# MAGIC FROM fato_pedido f
# MAGIC JOIN dim_servico s USING (sk_servico)
# MAGIC GROUP BY s.servico
# MAGIC ORDER BY pedidos DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4. A chuva aumenta os pedidos? Média de pedidos abertos por dia, por faixa de chuva do dia

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.faixa_chuva,
# MAGIC        count(DISTINCT t.dt)                                   AS dias,
# MAGIC        count(f.sk_pedido)                                     AS pedidos,
# MAGIC        round(count(f.sk_pedido) / count(DISTINCT t.dt), 2)    AS pedidos_por_dia
# MAGIC FROM dim_tempo t
# MAGIC JOIN dim_clima c USING (sk_tempo)
# MAGIC LEFT JOIN fato_pedido f ON f.sk_tempo_abertura = t.sk_tempo
# MAGIC GROUP BY c.faixa_chuva
# MAGIC ORDER BY pedidos_por_dia DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q5. Por qual canal o pedido chega e isso muda o resultado? (a pergunta "tipo de lâmpada" foi descartada: 100% dos pontos do GeoSampa são LED)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ca.canal,
# MAGIC        count(*)                                                                   AS pedidos,
# MAGIC        round(100 * count_if(f.em_aberto) / count(*), 1)                           AS pct_abertos,
# MAGIC        percentile_approx(CASE WHEN f.status = 'FINALIZADA' THEN f.dias_atendimento END, 0.5) AS mediana_dias_atendidos
# MAGIC FROM fato_pedido f
# MAGIC JOIN dim_canal ca USING (sk_canal)
# MAGIC GROUP BY ca.canal
# MAGIC ORDER BY pedidos DESC;
