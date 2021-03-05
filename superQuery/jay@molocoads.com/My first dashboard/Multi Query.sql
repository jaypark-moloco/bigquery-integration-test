/*Run multiple queries in parallel or in a sequence.
Just separate each query with a semicolon, like in the example below.*/

SELECT local_date, advertiser_id, campaign_id,
ROUND(SUM(moloco_spent),2) AS moloco_spent, SUM(revenue) AS revenue, SUM(imp) AS imp
FROM `moloco-data-prod.looker.campaign_summary_all`
WHERE local_date BETWEEN "2021-02-24" AND "2021-02-27"
AND advertiser_id = "fourones"
AND campaign_id = "uwy4oQ7Kkxzr6mjc"
GROUP BY 1,2,3
ORDER BY 1