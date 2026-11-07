# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — Rua Acesa
# MAGIC Cria o schema e o Volume `chegada` no Databricks Free Edition.
# MAGIC
# MAGIC **Depois de rodar:** abra *Catalog → workspace → rua_acesa → chegada* e use **Upload to this volume**
# MAGIC para enviar os arquivos da pasta local `chegada/`, **mantendo as subpastas**:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/workspace/rua_acesa/chegada/sp156/sp156_AAAA-MM-DD.csv
# MAGIC /Volumes/workspace/rua_acesa/chegada/geosampa/iluminacao_AAAA-MM-DD.csv
# MAGIC /Volumes/workspace/rua_acesa/chegada/geosampa/distritos_AAAA-MM-DD.csv
# MAGIC /Volumes/workspace/rua_acesa/chegada/openmeteo/openmeteo_AAAA-MM-DD.json
# MAGIC ```
# MAGIC **Lote 1** = arquivos de um dia de coleta; **Lote 2** = arquivos de outro dia. Tire print do Volume com os dois.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.rua_acesa;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.rua_acesa.chegada;
# MAGIC USE CATALOG workspace;
# MAGIC USE SCHEMA rua_acesa;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conferir o conteúdo do Volume (print do requisito 04)

# COMMAND ----------

for pasta in ["sp156", "geosampa", "openmeteo"]:
    print("==", pasta)
    for f in dbutils.fs.ls(f"/Volumes/workspace/rua_acesa/chegada/{pasta}/"):
        print(f"{f.size:>12,}  {f.name}")
