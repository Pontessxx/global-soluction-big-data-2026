# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · LGPD e entrega ao MVP (requisito 08)
# MAGIC **Tabela da Ouro que o MVP usa:** `workspace.rua_acesa.ouro_mvp_ranking_distritos`
# MAGIC (agregada por distrito, sem nenhum dado individual).
# MAGIC **Tela:** `mvp/index.html`, que lê o CSV exportado por este notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Evidência LGPD — campo mascarado na Ouro (print)
# MAGIC Compare a Prata (endereço em claro) com a Ouro (hash, CEP mascarado, coordenada arredondada).

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC USE SCHEMA rua_acesa;
# MAGIC
# MAGIC SELECT 'PRATA (uso interno, não exposta)' AS camada, chave AS id, logradouro, numero, cep, lat, lon, cast(NULL AS STRING) AS endereco_hash
# MAGIC FROM prata_sp156 LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'OURO (exposta ao produto)' AS camada, sk_pedido AS id, endereco_hash, cep_mascarado, lat_aprox, lon_aprox, status
# MAGIC FROM fato_pedido LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Prova: a fato não tem coluna de endereço exato nem coordenada exata
# MAGIC DESCRIBE TABLE fato_pedido;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Exportar a tabela do MVP (CSV)
# MAGIC Baixe o arquivo pela interface (*Catalog → Volumes → exports*) e salve como `mvp/dados/ouro_ranking_distritos.csv`.

# COMMAND ----------

import os
os.makedirs("/Volumes/workspace/rua_acesa/chegada/exports", exist_ok=True)
df = spark.table("workspace.rua_acesa.ouro_mvp_ranking_distritos").orderBy("pedidos", ascending=False)
df.toPandas().to_csv("/Volumes/workspace/rua_acesa/chegada/exports/ouro_ranking_distritos.csv",
                     index=False, encoding="utf-8")
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaração LGPD (copiar para o relatório)
# MAGIC * O SP156 aberto **não contém nome, CPF, telefone ou e-mail** de quem abriu o pedido (conferido nas 15 colunas do arquivo).
# MAGIC * O endereço do local do problema pode identificar uma residência; por isso a Ouro guarda só hash com sal, CEP parcial e coordenada com ~110 m de precisão.
# MAGIC * GeoSampa e Open-Meteo são dados de infraestrutura e clima, sem pessoa.
# MAGIC * O sal do hash é informado no widget do notebook 03 e **não é versionado**.
