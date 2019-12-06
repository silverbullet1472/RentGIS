import psycopg2

ls=[]
conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
cur = conn.cursor()
cur.executemany("insert into jiaozuo_lease_house values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", ls)
conn.commit()
conn.close()