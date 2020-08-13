#kafka
from pykafka import KafkaClient
client = KafkaClient(hosts="127.0.0.1:9092")
topic = client.topics["test4"]
consumer = topic.get_simple_consumer()

#cassandra
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()
session.execute("USE zh;")
#print("message_id" , "timestampISO" , "channel_id", "hashtags")
for message in consumer:
    if message is not None:
        #print(message.offset, (message.value).decode("utf-8") )
        msg = (message.value).decode("utf-8") 
        channel_id = ''
        message_id = ""
        timestampISO = ""
        hour = ""
        date = ""
        hashtags = ""
        if msg.find('message_id')!= -1:
                msg = msg[msg.find('message_id')+14:]
                message_id = msg[:msg.find(',')-1]
        if msg.find('timestampISO')!= -1:
                msg = msg[msg.find('timestampISO')+16:]
                timestampISO = msg[:msg.find(',')-1]
                date = msg[:msg.find('T')]
                hour = msg[msg.find('T')+1:msg.find(':')]
        if msg.find('channel_id')!= -1:
                msg = msg[msg.find('channel_id')+14:]
                channel_id = msg[:msg.find(',')-1]
        if msg.find('hashtags')!= -1:
                msg = msg[msg.find('hashtags')+12:]
                hashtags = msg[:msg.find('],')] 
        insert1 = """INSERT INTO all_posts(date, hour, message_id , channel_id) VALUES(%s,%s, %s, %s);"""
        session.execute(insert1,(date,hour,message_id,channel_id))
        
        insert2 = """INSERT INTO all_channels(channel_id ,date, hour, message_id) VALUES(%s,%s,%s,%s);"""
        session.execute(insert2,(channel_id,date,hour, message_id))
        
        x = hashtags.split(", ")
        for h in x:
                if h !="":
                        insert3 = """INSERT INTO all_hashtags(hashtag_name ,date, hour, message_id) VALUES(%s, %s,%s,%s);"""
                        session.execute(insert3,(h[1:-1],date,hour,message_id))

        print(message_id , timestampISO, date,hour, channel_id, hashtags)
        
