-- Procedure para atualizar views que serão usadas na IA
CREATE OR REPLACE PROCEDURE public.atualiza_views_IA()
LANGUAGE plpgsql
AS $$
BEGIN

--## View com KPI's por customer

	CREATE OR REPLACE VIEW public.gold_kpi_by_customer AS
	SELECT
	  c.customer_id,
	  c.customer_name,
	  SUM(e.notional_abs_usd)    AS volume_gross_usd,
	  SUM(e.notional_signed_usd) AS fluxo_liquido_usd,
	  COUNT(*)                   AS transacoes
	FROM historico.hist_sales_enriched e
	JOIN stage.stg_customers c
	  ON c.customer_id = e.cliente_id
	GROUP BY c.customer_id, c.customer_name;
	
--## View com vendas dos últimos 7 dias

	CREATE OR REPLACE VIEW public.gold_last7_assets_vendas AS
	SELECT
	  s.data_dia::date AS data,
	  s.symbol_cotacao_norm AS ativo,
	  SUM(s.notional_abs_usd) AS volume_vendas_usd
	FROM historico.hist_sales_enriched s
	WHERE s.data_dia >= current_date - INTERVAL '6 days'
	  AND s.tipo_operacao = 'VENDA'
	GROUP BY 1, 2;


END;
$$;
