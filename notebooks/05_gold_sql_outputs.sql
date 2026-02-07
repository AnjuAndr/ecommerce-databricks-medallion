

Notebook: 05_gold_sql_outputs
Layer   : Gold (Consumption)
Purpose : SQL outputs as requested in the task
Source  : gold.profit_aggregated


USE gold;

----------------------------------------------------

/*Profit By Year*/
SELECT 
  order_year as Year, 
  round(SUM(total_profit),2) AS profit 
FROM profit_aggregated
GROUP BY order_year 
ORDER BY order_year;

---------------------------------------------------


/*Profit by Year + Product Category*/
SELECT
  order_year,
  category,
  round(SUM(total_profit),2) AS profit
FROM profit_aggregated
group by order_year, category
ORDER BY order_year, category;


-----------------------------------------------------
/*Profit by Customer*/

select 
  customer_name,
  round(sum(total_profit), 2) as profit
from gold.profit_aggregated
group by customer_name
order by profit desc;

----------------------------------------------------------
/*Profit by Customer + Year*/

select 
  customer_name,
  order_year,
  round(sum(total_profit), 2) as profit
from gold.profit_aggregated
group by customer_name, order_year
order by customer_name, order_year;
    

