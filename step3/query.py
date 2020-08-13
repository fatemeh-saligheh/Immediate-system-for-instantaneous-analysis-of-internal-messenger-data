from cassandra.cluster import Cluster
from datetime import datetime

cluster = Cluster()
session = cluster.connect()
session.execute("USE zh;")
query1 = """select message_id from all_posts where 
date = ? and hour = ?;"""
st = session.prepare(query1)

now = datetime.now()
dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
x = dt_string.split(' ')
messages = session.execute(st, [x[0],x[1][:2]])
print("all message_ids in last hour")
for m in messages:
    print(m)
print("----------------\n")
query2 = """select message_id from all_posts where 
date = ?;"""
st2 = session.prepare(query2)

now = datetime.now()
dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
x = dt_string.split(' ')
messages = session.execute(st2, [x[0]])
print("all message_ids in last 24-hours")
for m in messages:
    print(m)
print("----------------\n")

query3 = """select message_id from all_hashtags where 
date = ? and hashtag_name = ?;"""
st3 = session.prepare(query3)

now = datetime.now()
dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
x = dt_string.split(' ')
messages = session.execute(st3, [x[0],"کرونا"])
print("all message_ids in last 24-hours that have #کرونا")
for m in messages:
    print(m)
print("----------------\n")

query4 = """select message_id from all_channels where 
date = ? and channel_id = ?;"""
st4 = session.prepare(query4)

now = datetime.now()
dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
x = dt_string.split(' ')
messages = session.execute(st4, [x[0],"farsna"])
print("all message_ids in last 24-hours  from farsna")
for m in messages:
    print(m)
print("----------------\n")

