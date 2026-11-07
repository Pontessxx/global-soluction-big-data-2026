# Rua Acesa — Big Data Architecture & Data Integration

Global Solution FIAP 2026.2 · CitizenTech · Engenharia de Software 4º ano

Pipeline de dados do MVP **Rua Acesa**: mostra em quais distritos de São Paulo a falha na infraestrutura de iluminação
pesa mais e quanto tempo a prefeitura leva para atender.

## Integrantes

| Nome completo | RM |
|---|---|
| Gabriel Diegues Figueiredo Rocha | 550788 |
| Henrique Pontes Oliveira | 98036 |
| Pedro Henrique Palladino | 551180 |
| Raí Gumieri dos Santos | 98287 |

## O problema e o que os dados permitem

O SP156 **não tem um serviço de "lâmpada apagada"**: iluminação pública é tratada fora do canal (conferido nos arquivos do
1º e 3º tri/2026, mais de 480 mil linhas por trimestre). O que existe, e é o mais próximo, são os serviços
**"Denunciar Poste em mau estado"** e **"Denunciar Fiação solta na calçada"**. Eles são o indicador de falha usado aqui,
e o GeoSampa dá o denominador (663 mil pontos de iluminação).

## Arquitetura

```
 SP156 (CKAN, CSV trimestral)  ─┐
 GeoSampa WFS: iluminação      ─┤   Airflow 3 (DAG gs_coleta, diário 06h)      chegada/<fonte>/
 GeoSampa WFS: distritos       ─┼─► 4 coletas em paralelo ► conferir_arquivos ► <fonte>_AAAA-MM-DD.ext
 Open-Meteo (chuva diária)     ─┘                                                    │
                                                                      upload manual pela interface (lotes)
                                                                                     ▼
              Volume Databricks ► Bronze (COPY INTO) ► Prata (MERGE + qualidade) ► Ouro (fato + dimensões)
                                                                                     │
                                                                          MVP: mvp/index.html (CSV da Ouro)
```

| Camada | Notebook | O que faz |
|---|---|---|
| Coleta | `dags/gs_coleta.py` | 4 tarefas paralelas + conferência + notificação de falha |
| Bronze | `databricks/01_bronze.py` | `COPY INTO` incremental, guarda arquivo de origem e hora; `controle_contagens` registra antes/depois |
| Prata | `databricks/02_prata.py` | tipagem, versão mais recente, 5 regras de qualidade com contagem, `MERGE` |
| Ouro | `databricks/03_ouro.py` | `fato_pedido` + `dim_tempo`, `dim_distrito`, `dim_servico`, `dim_canal`, `dim_clima`; LGPD |
| SQL | `databricks/04_consultas_sql.py` | as 5 consultas (também em `databricks/sql/consultas_ouro.sql`) |
| MVP/LGPD | `databricks/05_lgpd_mvp.py` | evidência LGPD e exportação da tabela do MVP |

## Fontes

| Fonte | Origem | Formato | Coleta | Chave de API |
|---|---|---|---|---|
| SP156 | dados.prefeitura.sp.gov.br (CKAN) | CSV trimestral (~100 MB, filtrado no DAG) | diária | não |
| GeoSampa iluminação | wfs.geosampa.prefeitura.sp.gov.br, camada `iluminacao_publica` | CSV via WFS (663 mil pontos) | diária | não |
| GeoSampa distritos | camada `distrito_municipal` | CSV com polígono WKT (96 distritos) | diária | não |
| Open-Meteo | archive-api.open-meteo.com | JSON (precipitação diária, 120 dias) | diária | não |

## Como reproduzir

**1. Coleta (Airflow 3)**
```bash
cp .env.example .env            # ajuste se quiser (nenhuma chave é obrigatória)
docker compose up -d            # http://localhost:8080  (usuário admin liberado localmente)
```
Ative o DAG `gs_coleta` e deixe rodar pelo agendamento (06h). Para testar na hora: *Trigger*. Os arquivos aparecem em `chegada/`.
Sem Docker, dá para testar as funções: `pip install -r requirements-local.txt && python scripts/coleta_local.py`.

**2. Databricks Free Edition** (o Free Edition não conecta ao Airflow, então o envio é manual)
1. Importe a pasta `databricks/` como notebooks e rode `00_setup`.
2. Em *Catalog → workspace → rua_acesa → chegada*, **Upload to this volume** mantendo as subpastas `sp156/`, `geosampa/`, `openmeteo/`.
   - **Lote 1:** arquivos de um dia de coleta. **Lote 2:** arquivos de outro dia.
3. `01_bronze` com `ROTULO = "lote1"`; suba o lote 2 e rode com `"lote2"`; rode de novo com `"lote2_reexecucao"`.
4. `02_prata` → `03_ouro` (preencha o widget `salt`) → `04_consultas_sql` → `05_lgpd_mvp`.

**3. MVP**
```bash
cd mvp && python -m http.server 8000   # http://localhost:8000
```
Salve o CSV exportado pelo notebook 05 em `mvp/dados/ouro_ranking_distritos.csv`. Sem ele, a página mostra uma amostra local.

## Estrutura do repositório

```
dags/         DAG gs_coleta e módulo rua_acesa/coleta.py
databricks/   notebooks 00–05 e sql/consultas_ouro.sql
amostras/     um pouco de cada fonte (dados reais de 05/11/2026)
mongodb/      não usado
mvp/          tela do MVP e dados de exemplo
scripts/      coleta local e gerador de amostras
```

## LGPD e segurança

* Sem chaves, senhas ou dados pessoais reais no repositório. `.env` e `chegada/` estão no `.gitignore`.
* O SP156 aberto não traz nome, CPF, telefone ou e-mail. Na Ouro, o endereço do local é guardado só como hash com sal,
  CEP parcial (`02977-***`) e coordenada arredondada (~110 m). O sal não é versionado.
