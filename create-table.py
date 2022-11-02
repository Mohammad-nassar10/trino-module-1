from module.trino_op import connect_user, execute_query


def create_table(user, table_name, schema_name, bucket_name):
    print("show catalogs")
    cur = connect_user(user, "iceberg", "localhost", 8080)
    res = execute_query(cur, "SHOW CATALOGS")
    print(res)

    print("create schema")
    location = "'s3a://" + bucket_name + "/'"
    schema_query = "create schema hive." + schema_name + " with (location = " + location + ")"
    print(schema_query)
    # schema_query = "create schema hive.icebergtrino with (location = 's3a://fybric-objectstorage-iceberg-demo/warehouse/db/')"

    res = execute_query(cur, schema_query)
    print(res)
    
    table_path = "iceberg." + schema_name + "." + table_name 
    create_table_query = "create table " + table_path + " (\
        a DOUBLE,\
        b DOUBLE,\
        c DOUBLE,\
        d DOUBLE\
        )\
        with (format = 'PARQUET')"

    res = execute_query(cur, create_table_query)
    print(res)

    print("insert rows to table")
    insert_query = "INSERT INTO " + table_path + " VALUES\
                    (\
                        1,\
                        2,\
                        3,\
                        4\
                    )"

    res = execute_query(cur, insert_query)
    print(res)


if __name__ == '__main__':
    create_table("admin", "tb1", "icebergtrino", "iceberg")
