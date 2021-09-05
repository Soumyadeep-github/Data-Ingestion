CREATE TABLE IF NOT EXISTS public.products_table(
                        name VARCHAR(100),
                        sku VARCHAR(200) NOT NULL,
                        description VARCHAR(20000),
                        PRIMARY KEY (sku, name)
                        );
                        
CREATE TABLE IF NOT EXISTS public.products_staging_table(
                            name VARCHAR(100),
                            sku VARCHAR(200) NOT NULL,
                            description VARCHAR(20000)
                            );
CREATE TABLE IF NOT EXISTS public.products_count_table(
                        name VARCHAR(100) NOT NULL,
                        number_of_records VARCHAR(200),
                        PRIMARY KEY (name)
                        );
CREATE TABLE IF NOT EXISTS public.count_table_stg(
                        name VARCHAR(100) NOT NULL,
                        number_of_records VARCHAR(200)
                        );

SELECT COUNT(*) FROM public.products_table;

SELECT COUNT(*) FROM public.products_staging_table;
                       