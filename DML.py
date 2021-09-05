class DML:
    COMMANDS = {}

    # COMMANDS['Insert_Products'] = """
    #                                 INSERT OR REPLACE  INTO products_table (name, sku, description)
    #                                 SELECT name, sku, description FROM products_staging_table
    #                                 EXCEPT
    #                                 SELECT name, sku, description FROM products_table;
    #                                 """
    # COMMANDS['Insert_Products'] = """
    #                                 INSERT OR REPLACE  INTO products_table (name, sku, description)
    #                                 SELECT name, sku, description FROM products_staging_table
    #                                 EXCEPT
    #                                 SELECT name, sku, description FROM products_table
    #                                 WHERE 1
    #                                 ON CONFLICT (sku, name) DO UPDATE SET
    #                                 description = EXCLUDED.description;
    #                                 """
    COMMANDS['Insert_Products'] = """
                                INSERT INTO products_table (name, sku, description)
                                SELECT name, sku, description FROM products_staging_table
                                EXCEPT
                                SELECT name, sku, description FROM products_table
                                WHERE TRUE
                                ON CONFLICT (sku, name) DO UPDATE SET
                                description = EXCLUDED.description;
                                """
    COMMANDS['Insert_Count_Staging'] = """
                                        INSERT INTO count_table_stg (name, number_of_records)
                                        SELECT name, COUNT(sku) AS number_of_records
                                        FROM products_staging_table
                                        GROUP BY name;
                                        """
    # COMMANDS['Insert_Products_Count'] = """
    #                                     INSERT INTO products_count_table (name, number_of_records)
    #                                     SELECT name, number_of_records FROM count_table_stg
    #                                     EXCEPT
    #                                     SELECT name, number_of_records FROM products_count_table
    #                                     WHERE 1
    #                                     ON CONFLICT (name) DO UPDATE SET 
    #                                     number_of_records = number_of_records + EXCLUDED.number_of_records;
    #                                     """

    COMMANDS['Insert_Products_Count'] = """
                                    INSERT INTO products_count_table (name, number_of_records)
                                    SELECT name, number_of_records FROM count_table_stg
                                    EXCEPT
                                    SELECT name, number_of_records FROM products_count_table
                                    ON CONFLICT (name) DO UPDATE SET 
                                    number_of_records = products_count_table.number_of_records::INT + EXCLUDED.number_of_records::INT;
                                    """


    COMMANDS['Flush_Staging_tables'] = """
                                        DELETE FROM products_staging_table;"""
    COMMANDS['Flush_Staging_tables_'] = """
                                        DELETE FROM count_table_stg;
                                        """
