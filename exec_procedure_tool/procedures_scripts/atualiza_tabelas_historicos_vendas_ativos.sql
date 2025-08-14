-- PROCEDURE: historico.atualiza_tabelas_historicos_vendas_ativos()

-- DROP PROCEDURE IF EXISTS historico.atualiza_tabelas_historicos_vendas_ativos();

CREATE OR REPLACE PROCEDURE historico.atualiza_tabelas_historicos_vendas_ativos(
	)
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN

    --##########################################################
    --### 1️⃣ Tabela histórico prices_hourly
    --##########################################################

    DROP TABLE IF EXISTS historico.hist_prices_hourly CASCADE;

    CREATE TABLE historico.hist_prices_hourly AS
    SELECT DISTINCT ON (ativo, moeda, date_trunc('hour', horario_coleta))
           ativo,
           moeda,
           date_trunc('hour', horario_coleta) AS hora,
           preco,
           horario_coleta AS cotacao_ts,
           id AS cotacao_id
    FROM stage.stg_cotacoes
    ORDER BY ativo, moeda, date_trunc('hour', horario_coleta), horario_coleta DESC;

    CREATE INDEX IF NOT EXISTS idx_hist_hourly_key
      ON historico.hist_prices_hourly (ativo, moeda, hora);

    --##########################################################
    --### 2️⃣ Tabela histórico sales_normalized
    --##########################################################

    DROP TABLE IF EXISTS historico.hist_sales_normalized CASCADE;

    CREATE TABLE historico.hist_sales_normalized AS
    SELECT
        b.transaction_id,
        date_trunc('hour', b.data_hora) AS data_hora_h,
        (b.data_hora AT TIME ZONE 'UTC')::date AS data_dia,
        EXTRACT(YEAR  FROM b.data_hora)::int AS ano,
        EXTRACT(MONTH FROM b.data_hora)::int AS mes,
        EXTRACT(DAY   FROM b.data_hora)::int AS dia,
        EXTRACT(HOUR  FROM b.data_hora)::int AS hora,
        TRIM(b.ativo) AS asset_raw,
        CASE
            WHEN UPPER(TRIM(b.ativo)) = 'BTC'    THEN 'BTC-USD'
            WHEN UPPER(TRIM(b.ativo)) = 'GOLD'   THEN 'GC=F'
            WHEN UPPER(TRIM(b.ativo)) = 'OIL'    THEN 'CL=F'
            WHEN UPPER(TRIM(b.ativo)) = 'SILVER' THEN 'SI=F'
            ELSE TRIM(b.ativo)
        END AS symbol_cotacao_norm,
        b.quantidade,
        b.tipo_operacao,
        b.moeda,
        b.cliente_id,
        b.canal,
        b.mercado
    FROM stage.stg_sales_btc_excel b

    UNION ALL

    SELECT
        c.transaction_id,
        date_trunc('hour', c.data_hora) AS data_hora_h,
        (c.data_hora AT TIME ZONE 'UTC')::date AS data_dia,
        EXTRACT(YEAR  FROM c.data_hora)::int AS ano,
        EXTRACT(MONTH FROM c.data_hora)::int AS mes,
        EXTRACT(DAY   FROM c.data_hora)::int AS dia,
        EXTRACT(HOUR  FROM c.data_hora)::int AS hora,
        TRIM(c.commodity_code) AS asset_raw,
        CASE
            WHEN UPPER(TRIM(c.commodity_code)) = 'BTC'    THEN 'BTC-USD'
            WHEN UPPER(TRIM(c.commodity_code)) = 'GOLD'   THEN 'GC=F'
            WHEN UPPER(TRIM(c.commodity_code)) = 'OIL'    THEN 'CL=F'
            WHEN UPPER(TRIM(c.commodity_code)) = 'SILVER' THEN 'SI=F'
            ELSE TRIM(c.commodity_code)
        END AS symbol_cotacao_norm,
        c.quantidade,
        c.tipo_operacao,
        c.moeda,
        c.cliente_id,
        c.canal,
        c.mercado
    FROM stage.stg_sales_commodities_sql c;

    ALTER TABLE historico.hist_sales_normalized
        ADD COLUMN silver_norm_id BIGSERIAL PRIMARY KEY;

    CREATE INDEX IF NOT EXISTS idx_silver_norm_asset_hora
      ON historico.hist_sales_normalized (symbol_cotacao_norm, data_hora_h);

    --##########################################################
    --### 3️⃣ Tabela histórico sales_enriched
    --##########################################################

    DROP TABLE IF EXISTS historico.hist_sales_enriched CASCADE;

    CREATE TABLE historico.hist_sales_enriched AS
    SELECT
      n.transaction_id,
      n.data_hora_h,
      n.data_dia, n.ano, n.mes, n.dia, n.hora,
      n.asset_raw,
      n.symbol_cotacao_norm,
      n.quantidade,
      n.tipo_operacao,
      n.moeda,
      n.cliente_id,
      n.canal,
      n.mercado,
      p.cotacao_id,
      p.cotacao_ts,
      p.preco AS preco_unitario_usd,
      n.quantidade * p.preco AS notional_abs_usd,
      (CASE WHEN n.tipo_operacao = 'VENDA' THEN 1
            WHEN n.tipo_operacao = 'COMPRA' THEN -1
            ELSE NULL END) * (n.quantidade * p.preco) AS notional_signed_usd
    FROM historico.hist_sales_normalized n
    LEFT JOIN historico.hist_prices_hourly p
      ON p.ativo = n.symbol_cotacao_norm
     AND p.moeda = 'USD'
     AND p.hora  = n.data_hora_h;

    ALTER TABLE historico.hist_sales_enriched
      ADD COLUMN silver_enriched_id BIGSERIAL PRIMARY KEY;

    CREATE INDEX IF NOT EXISTS idx_silver_enriched_asset_hora
      ON historico.hist_sales_enriched (symbol_cotacao_norm, data_hora_h);

END;
$BODY$;
ALTER PROCEDURE historico.atualiza_tabelas_historicos_vendas_ativos()
    OWNER TO postgres;
