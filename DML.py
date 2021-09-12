from settings import *

class DML:

    def __init__(self):
        self.STATEMENTS = {}
    
    def upsert_statements(self):
        """Generate statements for inserting data from a source (staging) table into target table.
            
            col
        """

        dml_query = ""
        insert_statement = f"INSERT INTO {MAIN_TABLE_NAME} "
        conflict_clause = "" 
        insert_statement += f"({', '.join(list(MAIN_COLUMN_NAMES.keys()))})"
        update_set = ""

        if KEY_COLUMNS != [] and UPDATE_COLUMNS != {}:
            length_update_keys = len(UPDATE_COLUMNS.keys())
            update_keys = list(UPDATE_COLUMNS.keys())
            if SCD_TYPE == 1:        
                for i, col in enumerate(list(UPDATE_COLUMNS.keys())):
                    update_set += f"{col} = EXCLUDED.{col}"
                    if i == length_update_keys-1:
                        update_set += " ;"
                    elif i <= length_update_keys-2:
                        update_set += ", "

            elif SCD_TYPE == 2 and SCD_COLUMNS != {} and SCD_UPDATE_MAPPING != {}: 
                for i, sc_co in zip(range(length_update_keys), SCD_UPDATE_MAPPING.keys()):
                    update_set += f"{SCD_UPDATE_MAPPING[sc_co]} = EXCLUDED.{sc_co}"
                    if i == length_update_keys-1:
                        update_set += " ;"
                    elif i <= length_update_keys-2:
                        update_set += ", "

            conflict_clause += f"ON CONFLICT ({', '.join(set(KEY_COLUMNS))}) DO UPDATE SET " + update_set

        elif KEY_COLUMNS and UPDATE_COLUMNS == {} and not SCD_TYPE:
            conflict_clause += f"ON CONFLICT ({', '.join(set(KEY_COLUMNS))}) DO NOTHING;"

        select_into = "SELECT {names} FROM {tbl_name}".format(names=', '.join(MAIN_COLUMN_NAMES.keys()),
                                                              tbl_name=MAIN_TABLE_NAME)

        select_from = "SELECT {names} FROM {stg_tblname}".format(names=', '.join(MAIN_COLUMN_NAMES.keys()),
                                                                stg_tblname=MAIN_STAGING_TABLE_NAME)

        dml_query += f"""
                        {insert_statement}
                        {select_into} 
                        EXCEPT
                        {select_from}
                        {conflict_clause}
                        """
        self.STATEMENTS["UPSERT_STATEMENT"] = dml_query
        self.STATEMENTS["DELETE_QUERIES"] = f"""DELETE FROM {MAIN_STAGING_TABLE_NAME}"""
        # return dml_query
    
    def insert_count(self):
        dml_query = ""
        insert_statement = "INSERT INTO {} ({})".format(MAIN_TABLE_COUNT, ', '.join(COUNT_COLUMN_NAMES.keys()))
        aggregations = []
        select_cols = ""

        group_by = "GROUP BY "
        if AGGREGATION:

            for aggregate_type, aggregate_column in AGGREGATION.items():
                try:
                    aggregations += [f"{aggregate_type}({aggregate_column}) AS {ALIAS_AGGREGATE_MAPPING[aggregate_column]}"]
                except:
                    aggregations += [f"{aggregate_type}({aggregate_column}) "]

            group_by += ', '.join(GROUP_BY_COLUMNS)

            select_cols += " {names}, ".format(names=', '.join(GROUP_BY_COLUMNS)) + ", ".join(aggregations)
        else:
            select_cols += " {names}".format(names=', '.join(COUNT_COLUMN_NAMES.keys()))

        select = "SELECT {} ".format(select_cols)
        from_tbl = "FROM {}".format(MAIN_TABLE_NAME)

        dml_query += f"""
                        {insert_statement}
                        {select}
                        {from_tbl}
                        {group_by};
                        """
        self.STATEMENTS["COUNT_INSERT_STATEMENT"] = dml_query
        # return dml_query
    
    def insert_raw_data(self):
        dml_query = ""
        insert_statement = "INSERT INTO {}".format(RAW_DATA_TABLE)
        select_statement = "SELECT * FROM {};".format(MAIN_STAGING_TABLE_NAME)
        dml_query += f"""
                        {insert_statement}
                        {select_statement};
                        """
        self.STATEMENTS["RAW_DATA_INSERT"] = dml_query
        # return dml_query
    

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

    # def run(self):
    #     self.upsert_statements()
    #     self.insert_count()
    #     self.insert_raw_data()
    #     DML_QUERIES = self.STATEMENTS
    #     # for i in DML_QUERIES:
    #     #     print(i)
    #     print(12)
    #     return DML_QUERIES
# if __name__ == '__main__':
# def run():
dml_instance = DML()
dml_instance.upsert_statements()
dml_instance.insert_count()
dml_instance.insert_raw_data()
DML_QUERIES = dml_instance.STATEMENTS
# for i in DML_QUERIES:
    # print(i)
    # return DML_QUERIES
print(1)

