from iotdb.dbapi import connect

conn = connect("127.0.0.1", 6667)
if conn.is_close:
    print("can't create connect")
    exit(1)
cursor = conn.cursor()

# execute test
cursor.execute("create storage group root.cursor")
cursor.execute("create storage group root.cursor_s1")