-- Copyright 2021 Google LLC.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

/*

[MANGO] Creating table no. 1: top_products

HOW TO:

1. Replace all instances of the following parameters - you can simply use the find/replace all function to do so.
      {country} - The country (only one) you're interested in (example DE)
      {dataset} - The name of the dataset into which you stored the Merchant Center data transfer (example merchant_center_dataset)
      {gmc_id} - Your Merchant Center's ID (example 123456789)
      {locale} - The language you want the data to be displayed in (examples: de-DE or us-EN)
      {project_id} - Your BigQuery Project's name (example: my_bigquery_project)
   After replacing all the parameters, the validator (top right corner) should approve the script (green circle with white checkmark). If not, please verify and correct parameter values.

2. Define name of the table you want the query to be stored in. You can do so by following these steps:
      a. Click "More" > "Query Settings" > "Set a destination table for query results"
      b. Specify table name to be "top_products" >
      c. Select "Overwrite table"
      d. Click "Save"

3. Run the query. You should now be able to see the "top_products table in your specified dataset"

4. [OPTIONAL] Schedule query if you want the data to update regularly. You can do so by following these steps:
      a. Click "Schedule" > "Create new scheduled query"
      b. Specify a name for your scheduled query (example: "MANGO top products")
      c. Specify schedule frequency (weekly recommended)
      d. Select "Overwrite table"
*/

WITH
  BestSellers AS (
    SELECT DISTINCT
      rank_timestamp,
      rank,
      rank_id,
      previous_rank,
      ranking_country,
      ranking_category,
      brand,
      google_brand_id,
      google_product_category,
      (
        SELECT name
        FROM TP.ranking_category_path
        WHERE locale = '{locale}'
      ) AS category_name,
      (
        SELECT name
        FROM TP.product_title
        LIMIT 1
      ) AS product_name,
      ARRAY_TO_STRING(gtins, ', ') AS gtins,
      price_range.min AS price_range_min,
      price_range.max AS price_range_max,
      price_range.currency AS price_range_currency,
      relative_demand.bucket AS relative_demand_bucket,
      _PARTITIONDATE AS PARTITIONDATE
    FROM `{project_id}.{dataset}.BestSellers_TopProducts_{gmc_id}` AS TP
    WHERE
      rank_timestamp = (
        SELECT MAX(rank_timestamp)
        FROM `{project_id}.{dataset}.BestSellers_TopProducts_{gmc_id}`
        WHERE DATE(_PARTITIONTIME) IS NOT NULL
      )
      AND ranking_country = '{country}'
      AND rank_id LIKE '%:{country}:%'
  ),
  Inventory AS (
    SELECT DISTINCT
      rank_id,
      product_id,
      merchant_id,
      aggregator_id
    FROM `{project_id}.{dataset}.BestSellers_TopProducts_Inventory_{gmc_id}`
    WHERE
      DATE(_PARTITIONTIME) = (
        SELECT MAX(DATE(_PARTITIONTIME))
        FROM `{project_id}.{dataset}.BestSellers_TopProducts_Inventory_{gmc_id}`
        WHERE _partitiontime IS NOT NULL
      )
      AND rank_id LIKE (
        CONCAT(
          (
            SELECT MAX(CAST(rank_timestamp AS Date))
            FROM `{project_id}.{dataset}.BestSellers_TopProducts_{gmc_id}`
            WHERE DATE(_PARTITIONTIME) IS NOT NULL
          ), ':{country}:%')
      )
      AND product_id LIKE '%:{country}:%'
  ),
  PriceBenchmarks AS (
    SELECT
      I.rank_id,
      price_benchmark_currency,
      AVG(price_benchmark_value) AS price_benchmark_value
    FROM `{project_id}.{dataset}.Products_PriceBenchmarks_{gmc_id}`
    LEFT JOIN
      `{project_id}.{dataset}.BestSellers_TopProducts_Inventory_{gmc_id}` AS I
      USING (product_id)
    WHERE
      price_benchmark_timestamp = (
        SELECT MAX(price_benchmark_timestamp)
        FROM `{project_id}.{dataset}.Products_PriceBenchmarks_{gmc_id}`
        WHERE _partitiontime IS NOT NULL
      )
    GROUP BY 1, 2
  ),
  InventoryPriceBenchmarks AS (
    SELECT *
    FROM Inventory
    LEFT JOIN PriceBenchmarks
      USING (rank_id)
  ),
  Price AS (
    SELECT
      I.rank_id,
      P.price.currency AS price_currency,
      COUNT(DISTINCT P.price.value) AS variants,
      AVG(P.price.value) AS price_value
    FROM `{project_id}.{dataset}.Products_{gmc_id}` AS P
    LEFT JOIN
      `{project_id}.{dataset}.BestSellers_TopProducts_Inventory_{gmc_id}` AS I
      USING (product_id)
    GROUP BY 1, 2
  )
SELECT DISTINCT
  rank_timestamp,
  rank,
  BS.rank_id,
  previous_rank,
  ranking_country,
  ranking_category,
  brand,
  google_brand_id,
  google_product_category,
  category_name,
  product_name,
  gtins,
  price_range_min,
  price_range_max,
  price_range_currency,
  relative_demand_bucket,
  PARTITIONDATE,
  merchant_id,
  aggregator_id,
  product_id,
  CASE
    WHEN product_id IS NULL THEN 'No'
    ELSE 'Yes'
    END AS inventory,
  PB.price_benchmark_value,
  PB.price_benchmark_currency,
  P.price_value,
  P.price_currency,
  P.variants
FROM BestSellers AS BS
LEFT JOIN InventoryPriceBenchmarks AS IPB
  ON BS.rank_id = IPB.rank_id
LEFT JOIN PriceBenchmarks AS PB
  ON PB.rank_id = BS.rank_id
LEFT JOIN Price AS P
  ON P.rank_id = BS.rank_id;
