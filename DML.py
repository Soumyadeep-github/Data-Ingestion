class DML:

    def __init__(self):
        self.STATEMENTS = {}
    
    def upsert_statements(self, tbl_name:str, 
                            col_names:list,
                            stg_cols:list, 
                            stg_tblname:str,
                            update:str=None, 
                            scd_type:str=None,
                            scd_cols:list=None,
                            key_columns:list=None):
        """Generate statements for inserting data from a source (staging) table into target table.
            
            col
        """

        dml_query = ""
        insert_statement = f"INSERT INTO {tbl_name} "
        conflict_clause = "" 
        insert_statement += f"({', '.join(col_names)})"
        update_set = ""

        if key_columns and update:

            if scd_type is "1":        
                for i, col in enumerate(update):
                    update_set += f"{col} = EXCLUDED.{col}"
                    if i == len(update)-1:
                        update_set += ";"
                    elif i <= len(update)-2:
                        update_set += ","

            elif scd_type == "2" and scd_cols: 
                for i, sc_co in zip(range(len(update)), scd_cols):
                    update_set += f"{sc_co} = EXCLUDED.{update[i]}"
                    if i == len(update)-1:
                        update_set += " ;"
                    elif i <= len(update)-2:
                        update_set += ", "

            conflict_clause += f"ON CONFLICT ({', '.join(set(key_columns))}) DO UPDATE SET " + update_set

        elif key_columns and not update and not scd_type:
            conflict_clause += f"ON CONFLICT ({', '.join(set(key_columns))}) DO NOTHING;"

        select_into = "SELECT {names} FROM {tbl_name}".format(names=', '.join(col_names),
                                                              tbl_name=tbl_name)

        select_from = "SELECT {names} FROM {stg_tblname}".format(names=', '.join(stg_cols),
                                                                stg_tblname=stg_tblname)

        except_clause = f"""
                        {select_into} 
                        EXCEPT
                        {select_from}
                        """
        dml_query += insert_statement + except_clause + conflict_clause
        return dml_query
    
    def insert_statements(self,
                            tblname_to:str,
                            cols_to:list,
                            tblname_from:str,
                            cols_from:list,
                            aggregate:list=None,
                            col_alias:dict=None):
        dml_query = ""
        insert_statement = "INSERT INTO {} ({})".format(tblname_to, ', '.join(cols_to))
        aggregations = []
        tblcols_from = ""

        group_by = "GROUP BY "
        if aggregate:

            for aggregate_type, aggregate_column in aggregate:
                if col_alias:
                    aggregations += [f"{aggregate_type}({aggregate_column}) AS {col_alias[aggregate_column]}"]
                else:
                    aggregations += [f"{aggregate_type}({aggregate_column}) "]
            
            group_by += ', '.join(tblcols_from)

            cols_from += " {names}, ".format(names=', '.join(cols_from)) + ", ".join(aggregations)
        else:
            cols_from += " {names}".format(names=', '.join(cols_from))

        select_from = "SELECT {} FROM {}".format(cols_from, tblname_from)

        dml_query += f"""
                    {insert_statement}
                    {select_from}
                    {group_by};
                    """
        return dml_query
        
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
                                INSERT INTO  products_table (name, sku, description)
                                SELECT name, sku, description FROM  products_staging_table
                                EXCEPT
                                SELECT name, sku, description FROM  products_table
                                WHERE TRUE
                                ON CONFLICT (sku, name) DO UPDATE SET
                                description = EXCLUDED.description;
                                """

    # COMMANDS['Insert_Count_Staging'] = """
    #                                     INSERT INTO count_table_stg (name, number_of_records)
    #                                     SELECT name, COUNT(sku) AS  number_of_records
    #                                     FROM products_staging_table
    #                                     GROUP BY name;
    #                                     """

    COMMANDS['Insert_Products_Raw'] = """
                                      INSERT INTO products_raw
                                      SELECT * FROM products_staging_table;
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

    # COMMANDS['Insert_Products_Count'] = """
    #                                     INSERT INTO products_count_table (name, number_of_records)
    #                                     SELECT name, number_of_records FROM count_table_stg
    #                                     EXCEPT
    #                                     SELECT name, number_of_records FROM products_count_table
    #                                     ON CONFLICT (name) DO UPDATE SET 
    #                                     number_of_records = products_count_table.number_of_records::INT + EXCLUDED.number_of_records::INT;
    #                                     """

    COMMANDS['Insert_Products_Count'] = """
                                        INSERT INTO products_count_table (name, number_of_records)
                                        SELECT name, COUNT(sku) AS  number_of_records
                                        FROM products_table
                                        GROUP BY name;
                                        """

    COMMANDS['Flush_Staging_tables'] = """DELETE FROM  products_staging_table;"""

    COMMANDS['Flush_Staging_tables_'] = """DELETE FROM  count_table_stg;"""
