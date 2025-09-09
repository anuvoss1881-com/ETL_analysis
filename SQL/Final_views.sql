-- Example: total profit by sub_category (view)
CREATE OR REPLACE VIEW marts.v_profit_by_subcategory AS
SELECT p.sub_category, SUM(o.profit) AS total_profit
FROM marts.fct_orders o
LEFT JOIN marts.dim_product p ON o.product_key = p.product_key
GROUP BY p.sub_category
ORDER BY total_profit DESC;
