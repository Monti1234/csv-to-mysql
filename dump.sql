CREATE DATABASE units;
USE units;
CREATE TABLE sources 
( id INT NOT NULL AUTO_INCREMENT, creation DATE NOT NULL, ad_unit_name VARCHAR(255) NOT NULL,
 ad_unit_id INT NOT NULL,type_tag INT NOT NULL, revenue_source VARCHAR(255) NOT NULL,
  market VARCHAR(255) NOT NULL, queries INT NOT NULL, clicks INT NOT NULL,
   impressions INT NOT NULL, page_rpm INT NOT NULL, impression_rpm INT NOT NULL,
  true_revenue INT NOT NULL, coverage INT NOT NULL, ctr INT NOT NULL, PRIMARY KEY (id));

LOAD DATA LOCAL INFILE '/home/monte/Desktop/tech/tech/code_challenge.csv' INTO TABLE sources FIELDS TERMINATED BY ',' ENCLOSED BY '"'  LINES TERMINATED BY '\n' IGNORE 1 ROWS ( id, creation, ad_unit_name, ad_unit_id, type_tag, revenue_source, market, queries, clicks, impressions, page_rpm, impression_rpm, true_revenue, coverage, ctr) SET creation = STR_TO_DATE(creation, '%d/m%/%y');

SELECT * FROM sources;
