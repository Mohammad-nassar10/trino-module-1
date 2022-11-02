from trino.dbapi import connect
from sql_metadata import Parser


def execute_query(cur, query):
    cur.execute(query)
    # res = None
    try:
        res = cur.fetchall()
        return res
    except Exception as e:
        print(e)
    # return res

def connect_user(username, catalog, host="localhost", port=8080):
    conn = connect(host=host, port=port, user=username, catalog=catalog)
    # conn = trino.dbapi.connect(host='host.docker.internal', port=8080, user=username, catalog=catalog)
    # conn = trino.dbapi.connect(host='localhost', port=8080, user=username, catalog=catalog)
    cur = conn.cursor()
    return cur



def exec_query_trino(user, sql_query):
    print("show catalogs")
    cur = connect_user(user, "iceberg", "localhost", 8080)
    res = execute_query(cur, "SHOW CATALOGS")
    print(res)

    # get the requested table from the query
    tables = Parser(sql_query).tables
    print(tables)
    # replace tables with views
    for table in tables:
        sql_query = sql_query.replace(table, table+'-view')

    print("updated query")
    print(sql_query)
    res = execute_query(cur, sql_query)
    print(res)
    return res


    # print("select from the table")
    # select_query = 'select * from iceberg.icebergtrino.logs'
    # res = execute_query(cur, select_query)
    # print(res)

    # print("user1 select")
    # cur = connect_user("user1", "iceberg")
    # res = execute_query(cur, select_query)
    # print(res)

    # print("user1 select from the view")
    # select_query = 'select * from iceberg.icebergtrino.view1'
    # res = execute_query(cur, select_query)
    # print(res)


# if __name__ == '__main__':
#     exec_query_trino("admin", "select * from iceberg.icebergtrino.logs limit 10")