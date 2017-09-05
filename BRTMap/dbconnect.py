import pymysql

def connection():

    conn = pymysql.connect(host="localhost", user="root", passwd="423123", db="tedb")
    c = conn.cursor()

    return c, conn