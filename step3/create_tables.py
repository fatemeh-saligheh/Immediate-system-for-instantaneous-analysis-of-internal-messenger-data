from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()
session.execute("USE zh;")
table1 = """CREATE TABLE all_posts(date text , hour text, message_id text, channel_id text 
,PRIMARY KEY((date),hour, channel_id,message_id)
);"""
session.execute(table1)

table2 = """CREATE TABLE all_channels(channel_id text ,date text,hour text, message_id text 
,PRIMARY KEY((channel_id),date,hour,message_id)
);"""
session.execute(table2)

table3 = """CREATE TABLE all_hashtags(hashtag_name text,date text,hour text, message_id text
,PRIMARY KEY((hashtag_name),date,hour,message_id)
);"""
session.execute(table3)

# session.execute("DROP TABLE all_posts;")
# session.execute("DROP TABLE all_channels;")
# session.execute("DROP TABLE all_hashtags;")