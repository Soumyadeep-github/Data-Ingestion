from settings import DB_URL


class DDL:
    TABLES = {}
    TABLES['PRODUCTS_TABLE'] = """
                        CREATE TABLE IF NOT EXISTS public.products_table(
                        name VARCHAR(100),
                        sku VARCHAR(200) NOT NULL,
                        description VARCHAR(20000),
                        PRIMARY KEY (sku, name)
                        );
                    """
    TABLES['PRODUCTS_TABLE_STAGE'] = """
                            CREATE TABLE IF NOT EXISTS public.products_staging_table(
                            name VARCHAR(100),
                            sku VARCHAR(200) NOT NULL,
                            description VARCHAR(20000)
                            );
                           """

    TABLES['AGGREGATE_TABLE'] = """
                        CREATE TABLE IF NOT EXISTS public.products_count_table(
                        name VARCHAR(100) NOT NULL,
                        number_of_records VARCHAR(200),
                        PRIMARY KEY (name)
                        );
                      """
    TABLES['AGGREGATE_TABLE_STAGE'] = """
                        CREATE TABLE IF NOT EXISTS public.count_table_stg(
                        name VARCHAR(100) NOT NULL,
                        number_of_records VARCHAR(200)
                        );
                        """


if __name__ == "__main__":
    # queries_inst = list(DDL.TABLES.values())
    # query = "".join(queries_inst)
    # with psycopg2.connect(f"dbname={DDL_DATABASE} user={DDL_USERNAME} password={DDL_PASSWORD}") as conn:
    #     with conn.cursor() as cur:
    #         cur.execute(query)
    engine = create_engine(DB_URL)
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(DDL.TABLES['PRODUCTS_TABLE'])
            connection.execute(DDL.TABLES['PRODUCTS_TABLE_STAGE'])
            connection.execute(DDL.TABLES['AGGREGATE_TABLE'])
            connection.execute(DDL.TABLES['AGGREGATE_TABLE_STAGE'])
