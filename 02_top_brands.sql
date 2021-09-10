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

[MANGO] Creating table no. 2: top_brands

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
      b. Specify table name to be "top_brands" >
      c. Select "Overwrite table"
      d. Click "Save"

3. Run the query. You should now be able to see the "top_brands table in your specified dataset"

4. [OPTIONAL] Schedule query if you want the data to update regularly. You can do so by following these steps:
      a. Click "Schedule" > "Create new scheduled query"
      b. Specify a name for your scheduled query (example: "MANGO top brands")
      c. Specify schedule frequency (weekly recommended)
      d. Select "Overwrite table"
*/

WITH
  BrandRanking AS (
    SELECT DISTINCT
      rank_timestamp,
      rank_id,
      rank,
      previous_rank,
      ranking_category,
      (
        SELECT name
        FROM TP.ranking_category_path
        WHERE locale = '{locale}'
      ) AS category_name,
      brand,
      relative_demand.bucket AS relative_demand
    FROM `{project_id}.{dataset}.BestSellers_TopBrands_{gmc_id}` AS TP
    WHERE
      rank_timestamp = (
        SELECT MAX(rank_timestamp)
        FROM `{project_id}.{dataset}.BestSellers_TopBrands_{gmc_id}`
        WHERE DATE(_PARTITIONTIME) IS NOT NULL
      )
      AND ranking_country = '{country}'
      AND brand IS NOT NULL
  ),
  BrandProducts AS (
    SELECT DISTINCT
      P.rank_timestamp,
      B.ranking_category,
      (
        SELECT name
        FROM P.ranking_category_path
        WHERE locale = '{locale}'
      ) AS ranking_category_name,
      B.category_name,
      B.brand,
      P.rank_id,
      B.rank,
      B.previous_rank,
      B.relative_demand
    FROM `{project_id}.{dataset}.BestSellers_TopProducts_{gmc_id}` AS P
    LEFT JOIN BrandRanking AS B
      ON
        b.brand = p.brand
        AND b.ranking_category = p.ranking_category
        AND b.rank_timestamp = p.rank_timestamp
    WHERE
      p.rank_timestamp = (
        SELECT MAX(rank_timestamp)
        FROM `{project_id}.{dataset}.BestSellers_TopBrands_{gmc_id}`
        WHERE DATE(_PARTITIONTIME) IS NOT NULL
      )
      AND ranking_country = '{country}'
  ),
  Inventory AS (
    SELECT DISTINCT
      rank_id,
      product_id
    FROM `{project_id}.{dataset}.BestSellers_TopProducts_Inventory_{gmc_id}`
    WHERE
      rank_id LIKE (
        CONCAT(
          (
            SELECT MAX(CAST(rank_timestamp AS Date))
            FROM `{project_id}.{dataset}.BestSellers_TopProducts_{gmc_id}`
            WHERE DATE(_PARTITIONTIME) IS NOT NULL
          ), ':{country}:%')
      )
      AND product_id LIKE '%{country}%'
  )
SELECT
  rank,
  previous_rank,
  B.ranking_category,
  B.category_name,
  B.brand,
  B.relative_demand,
  COUNT(I.rank_id) AS products_in_inventory,
  COUNT(B.rank_id) AS all_products,
  COUNT(I.rank_id) / COUNT(B.rank_id) AS brand_coverage
FROM BrandProducts AS B
LEFT JOIN Inventory AS I
  ON B.rank_id = I.rank_id
GROUP BY 1, 2, 3, 4, 5, 6;
