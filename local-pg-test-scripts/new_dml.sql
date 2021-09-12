SELECT *
	FROM public.products_table;
	
SELECT * FROM public.products_count_table;	

INSERT INTO public.products_count_table (name, number_of_records)
SELECT name, COUNT(sku) AS number_of_records
FROM public.products_table
GROUP BY name;

CREATE DATABASE TestDB;

CREATE SCHEMA IF NOT EXISTS ingestion_pipeline AUTHORIZATION postgres;