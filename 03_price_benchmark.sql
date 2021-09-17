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

[Assortment Analytics] Creating table no. 3: price_benchmark

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
      b. Specify table name to be "price_benchmark"
      c. Select "Overwrite table"
      d. Click "Save"

3. Run the query. You should now be able to see the "price_benchmark table in your specified dataset"

4. [OPTIONAL] Schedule query if you want the data to update regularly. You can do so by following these steps:
      a. Click "Schedule" > "Create new scheduled query"
      b. Specify a name for your scheduled query (example: "Assortment Analytics price benchmark")
      c. Specify schedule frequency (weekly recommended)
      d. Select "Overwrite table"
*/

WITH
  TopProducts AS (
    SELECT DISTINCT
      product_name,
      brand,
      ranking_country,
      price_range_min,
      price_range_max,
      price_value,
      price_benchmark_value,
      inventory
    FROM `{project_id}.{dataset}.top_products`
  ),
  Benchmark AS (
    SELECT
      brand,
      ranking_country,
      COUNT(DISTINCT product_name) AS product_count,
      SUM(IF(inventory = 'Yes', 1, 0)) AS inventory_count,
      SUM(IF(price_benchmark_value IS NOT NULL, 1, 0)) AS benchmarked_count,
      (
        SUM(IF(price_value > price_benchmark_value, 1, 0))
        / NULLIF(SUM(IF(price_benchmark_value IS NOT NULL, 1, 0)), 0)
      ) AS above_benchmark,
      (
        SUM(IF(price_value = price_benchmark_value, 1, 0))
        / NULLIF(SUM(IF(price_benchmark_value IS NOT NULL, 1, 0)), 0)
      ) AS at_benchmark,
      (
        SUM(IF(price_value < price_benchmark_value, 1, 0))
        / NULLIF(SUM(IF(price_benchmark_value IS NOT NULL, 1, 0)), 0)
      ) AS below_benchmark
    FROM TopProducts
    GROUP BY 1, 2
  )
SELECT *
FROM Benchmark
WHERE inventory_count > 0;
