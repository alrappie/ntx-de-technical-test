-- Approach Step 1: check all data
use ntx;
SELECT * 
FROM `ecommerce-session-bigquery`;

-- Step 2: Channel Analysis:
-- Approach: use country and channel grouping as dimension, and use productRevenue as metric order by product revenue desc
select 
	country,
    channelGrouping,
    sum(productRevenue) as total_revenue
from `ecommerce-session-bigquery`
group by 1,2
order by 3 desc
limit 5;

-- Step 3:  User Behavior Analysis: 
-- Aporach: create CTE to get the avg user and create another CTE to get the total avg. Then compare those CTE
with avg_visitor as (
	select 
		fullVisitorId,
		avg(timeOnSite) as avg_timeOnSite,
		avg(pageviews) as avg_pageviews,
		avg(sessionQualityDim) as avg_sessionQualityDim
	from `ecommerce-session-bigquery`
	group by 1
)
,
avg_score as (
	select 
		avg(timeOnSite) as avg_timeOnSite_overall,
		avg(pageviews) as avg_pageviews_overall,
		avg(sessionQualityDim) as avg_sessionQualityDim_overall
	from `ecommerce-session-bigquery`
)
select 
	fullVisitorId
from avg_visitor
where 
	avg_timeOnSite > (select avg_timeOnSite_overall from avg_score)
    and avg_pageviews < (select avg_pageviews_overall from avg_score);

-- step 4: Product Performance 
-- Approach: productName as dimension, add 4 metrics and flag if the product refund surpassing of it's total revenue.
SELECT 
	v2ProductName,
	SUM(productRevenue) AS total_revenue,
	SUM(productQuantity) AS total_quantity_sold,
	SUM(productRefundAmount) AS total_refund_amount,
	(SUM(productRevenue) - SUM(productRefundAmount)) AS net_revenue,
	CASE
		WHEN SUM(productRefundAmount) > 0.1 * SUM(productRevenue) THEN 'Flagged'
		ELSE 'Not Flagged'
	END AS refund_flag
FROM `ecommerce-session-bigquery`
GROUP BY 1
ORDER BY 5 DESC;