-- Rua Acesa · 5 consultas SQL da Ouro (requisito 07)
-- Rode no Databricks (schema workspace.rua_acesa). Cada consulta responde uma pergunta do MVP.
-- Depois de rodar, cole o resultado e escreva a interpretação em relatorio/RELATORIO.md (seção 8).
-- Consulta de IMPACTO: Q2.
USE CATALOG workspace;
USE SCHEMA rua_acesa;

-- =====================================================================================
-- Q1. Quais distritos têm mais pedidos por mil pontos de iluminação?
--     (pedidos de poste em mau estado / fiação solta, normalizados pelo tamanho da rede)
-- =====================================================================================
SELECT d.nm_distrito, d.qt_pontos_iluminacao AS pontos,
       count(*)                                                AS pedidos,
       round(1000 * count(*) / d.qt_pontos_iluminacao, 2)      AS pedidos_por_mil_pontos
FROM fato_pedido f
JOIN dim_distrito d USING (sk_distrito)
WHERE d.qt_pontos_iluminacao > 0
GROUP BY d.nm_distrito, d.qt_pontos_iluminacao
ORDER BY pedidos_por_mil_pontos DESC
LIMIT 10;

-- =====================================================================================
-- Q2 (IMPACTO). Onde a espera é maior: quantos pedidos seguem abertos há mais de 30 dias
--     e qual a mediana dos que foram atendidos, por distrito?
-- =====================================================================================
SELECT d.nm_distrito,
       count(*)                                                                   AS pedidos,
       count_if(f.em_aberto)                                                      AS abertos,
       count_if(f.em_aberto AND f.dias_em_aberto > 30)                            AS abertos_mais_30d,
       round(100 * count_if(f.em_aberto) / count(*), 1)                           AS pct_abertos,
       count_if(f.status = 'FINALIZADA')                                          AS finalizados,
       percentile_approx(CASE WHEN f.status = 'FINALIZADA' THEN f.dias_atendimento END, 0.5) AS mediana_dias_atendidos
FROM fato_pedido f
JOIN dim_distrito d USING (sk_distrito)
GROUP BY d.nm_distrito
HAVING count(*) >= 20
ORDER BY abertos_mais_30d DESC
LIMIT 15;

-- =====================================================================================
-- Q3. O serviço (poste x fiação) muda o tempo de atendimento e a fila?
-- =====================================================================================
SELECT s.servico,
       count(*)                                                                   AS pedidos,
       count_if(f.status = 'FINALIZADA')                                          AS finalizados,
       round(100 * count_if(f.status = 'FINALIZADA') / count(*), 1)               AS pct_finalizados,
       percentile_approx(CASE WHEN f.status = 'FINALIZADA' THEN f.dias_atendimento END, 0.5) AS mediana_dias,
       percentile_approx(CASE WHEN f.status = 'FINALIZADA' THEN f.dias_atendimento END, 0.9) AS p90_dias
FROM fato_pedido f
JOIN dim_servico s USING (sk_servico)
GROUP BY s.servico
ORDER BY pedidos DESC;

-- =====================================================================================
-- Q4. A chuva aumenta os pedidos? Média de pedidos abertos por dia, por faixa de chuva do dia
-- =====================================================================================
SELECT c.faixa_chuva,
       count(DISTINCT t.dt)                                   AS dias,
       count(f.sk_pedido)                                     AS pedidos,
       round(count(f.sk_pedido) / count(DISTINCT t.dt), 2)    AS pedidos_por_dia
FROM dim_tempo t
JOIN dim_clima c USING (sk_tempo)
LEFT JOIN fato_pedido f ON f.sk_tempo_abertura = t.sk_tempo
GROUP BY c.faixa_chuva
ORDER BY pedidos_por_dia DESC;

-- =====================================================================================
-- Q5. Por qual canal o pedido chega e isso muda o resultado?
--     (a pergunta "tipo de lâmpada" foi descartada: 100% dos pontos do GeoSampa são LED)
-- =====================================================================================
SELECT ca.canal,
       count(*)                                                                   AS pedidos,
       round(100 * count_if(f.em_aberto) / count(*), 1)                           AS pct_abertos,
       percentile_approx(CASE WHEN f.status = 'FINALIZADA' THEN f.dias_atendimento END, 0.5) AS mediana_dias_atendidos
FROM fato_pedido f
JOIN dim_canal ca USING (sk_canal)
GROUP BY ca.canal
ORDER BY pedidos DESC;
