import psycopg2

#通过connect方法创建数据库连接
conn = psycopg2.connect(dbname="postgres", user="postgres",password="19960424", host="127.0.0.1", port="5432")

# 创建cursor以访问数据库
cur = conn.cursor()

#创建表
cur.execute('create table student(id serial primary key,student_name varchar(20),age int ,class_name varchar(20));')

#插入数据
cur.execute("insert into student (student_name,age,class_name) values('s1',18,'class1');\
insert into student (student_name,age,class_name) values('s2',22,'class1');\
insert into student (student_name,age,class_name) values('s3',18,'class2');")

#查询并打印(读取Retrieve)
cur.execute("select * from student")
rows = cur.fetchall()
print('--------------------------------------------------------------------------------------')
for row in rows:
    print('id=' + str(row[0]) + ' student_name=' + str(row[1]) +
        ' age=' + str(row[2]) + ' class_name=' + str(row[3]))
print('--------------------------------------------------------------------------------------\n')


# 更新数据(U)
cur.execute("update student set age=36,class_name='class_3' where id=2")
cur.execute("select * from student")
rows = cur.fetchall()
print('--------------------------------------------------------------------------------------')
for row in rows:
    print('id=' + str(row[0]) + ' student_name=' + str(row[1]) +
        ' age=' + str(row[2]) + ' class_name=' + str(row[3]))
print('--------------------------------------------------------------------------------------\n')

# 删除数据(D)
cur.execute("delete from student where id=1")
cur.execute("select * from student")
rows = cur.fetchall()
print('--------------------------------------------------------------------------------------')
for row in rows:
    print('id=' + str(row[0]) + ' student_name=' + str(row[1]) +
        ' age=' + str(row[2]) + ' class_name=' + str(row[3]))
print('--------------------------------------------------------------------------------------\n')

#删除表
cur.execute("drop table student")

# 提交事务
conn.commit()

# 关闭连接
conn.close()