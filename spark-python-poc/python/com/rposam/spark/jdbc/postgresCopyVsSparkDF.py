import time
import psycopg2

from com.rposam.spark.jdbc.iter_file import IteratorFile

# conn = psycopg2.connect(dbname="awspostgredb", host="postgres.cabrrax308iv.ap-south-1.rds.amazonaws.com",port=5432, user="postgresuser", password="password")
host="postgres.cabrrax308iv.ap-south-1.rds.amazonaws.com"
port=5432
user ="postgresuser"
password="password"
db="awspostgredb"

conn = psycopg2.connect(host = host,
        port = port,
        user = user,
        password = password,
        dbname = db)

table_name = "awspostgredb.temp"
# args = [(1,2), (3,4), (5,6)]
args = [(i,i+1) for i in range(1,1*10**6,2)]

def time_test(func):
    def f():
        truncate_table()
        t1 = time.time()
        func()
        t2 = time.time()
        print(str(t2 - t1) + ": " + func.__name__ )
    return f

def truncate_table():
    with conn.cursor() as cur:
        cur.execute("truncate table "+table_name);
        conn.commit()

@time_test
def query_builder_insert():
    with conn.cursor() as cur:
        records_list_template = ','.join(['%s'] * len(args))
        insert_query = "insert into "+table_name+" (a, b) values {0}".format(records_list_template)
        cur.execute(insert_query, args)
        conn.commit()
@time_test
def copy_from_insert():
    with conn.cursor() as cur:
        f = IteratorFile(("{}\t{}".format(x[0], x[1]) for x in args))
        cur.copy_from(f, table_name, columns=('a', 'b'))
        conn.commit()
# when query_builder_insert is executed, it will automatically pass invoking function to time_test
query_builder_insert()
copy_from_insert()