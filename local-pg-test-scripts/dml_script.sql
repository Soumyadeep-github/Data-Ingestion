INSERT INTO products_table (name, sku, description)
SELECT name, sku, description FROM products_staging_table
EXCEPT
SELECT name, sku, description FROM products_table
WHERE TRUE
ON CONFLICT (sku, name) DO UPDATE SET
description = EXCLUDED.description;

INSERT INTO count_table_stg (name, number_of_records)
                                        SELECT name, COUNT(sku) AS number_of_records
                                        FROM products_staging_table
                                        GROUP BY name;

INSERT INTO products_count_table (name, number_of_records)
SELECT name, number_of_records FROM count_table_stg
EXCEPT
SELECT name, number_of_records FROM products_count_table
ON CONFLICT (name) DO UPDATE SET 
number_of_records = products_count_table.number_of_records::INT + EXCLUDED.number_of_records::INT;