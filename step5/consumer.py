#kafka
from pykafka import KafkaClient
client = KafkaClient(hosts="127.0.0.1:9092")
topic = client.topics["test4"]
consumer = topic.get_simple_consumer()

#clickhouse
from clickhouse_driver import Client
client = Client(host='localhost')

from datetime import datetime
#print("message_id" , "timestampISO" , "channel_id", "hashtags")
for message in consumer:
    if message is not None:
        #print(message.offset, (message.value).decode("utf-8") )
        msg = (message.value).decode("utf-8") 
        channel_id = ''
        message_id = ""
        timestampISO = ""
        time_ = ""
        date = ""
        metadata = ""
        hashtags = ""
        date_time_obj = datetime.now()
        if msg.find('message_id')!= -1:
                msg = msg[msg.find('message_id')+14:]
                message_id = msg[:msg.find(',')-1]
        if msg.find('timestampISO')!= -1:
                msg = msg[msg.find('timestampISO')+16:]
                timestampISO = msg[:msg.find(',')-1]
                date = msg[2:msg.find('T')]
                time_ = msg[msg.find('T')+1:msg.find('+')]
                date_time_obj = datetime.strptime(date+" "+time_, '%y-%m-%d %H:%M:%S')
        if msg.find('channel_id')!= -1:
                msg = msg[msg.find('channel_id')+14:]
                channel_id = msg[:msg.find(',')-1]
        if msg.find('metadata')!= -1:
                msg = msg[msg.find('metadata')+12:]
                metadata = msg[:msg.find('],')] 
        if msg.find('hashtags')!= -1:
                msg = msg[msg.find('hashtags')+12:]
                hashtags = msg[:msg.find('],')] 
        insert1 = """INSERT INTO clickhouse.all_posts(
                channel_id,eventtime,message_id ,
                hashtags
                ,metadata
            ) VALUES"""
        x = hashtags.split(", ")
        y = metadata.split(", ")
        client.execute(insert1,[{'channel_id':channel_id,'eventtime':date_time_obj,'message_id':message_id,'metadata':y,'hashtags':x}])
        
        #print(message_id ,date_time_obj, channel_id, x,y)
        
