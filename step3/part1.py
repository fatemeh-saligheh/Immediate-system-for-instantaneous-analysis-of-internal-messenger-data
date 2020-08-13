# !python -m pip install pykafka
from pykafka import KafkaClient
# from kafka import KafkaProducer
# producer = KafkaProducer(bootstrap_servers='localhost:9092')
client = KafkaClient(hosts="127.0.0.1:9092")
topic = client.topics["test4"]

import hashlib
import requests
import json
from time import sleep
import sseclient
def get_channel_data(channel_name, size, last_message_id):
    endpoint = 'channel/archive'
    a = "95d58639"
    b = "24c0"
    c = "4b72"
    d = "80a5"
    f = "e124f41d7af9"
    api_secret = "-".join([a,b,c,d,f])
    api_resource = 'channel/archive'
    clinet_id = '7743461522282941752'
    format = 'json'
    api_version = 'v1'
    if last_message_id != -1:
        message = f'{{"channel_id":"{channel_name}", "limit": {size}, "message_id": "{last_message_id}"}}'
    else:
        message = f'{{"channel_id":"{channel_name}", "limit": {size}}}'
    digest = hashlib.md5((api_secret+api_version+format+clinet_id+api_resource+message).encode('utf-8')).hexdigest()
    url = f"https://what.sapp.ir/srvcs-app/v1/json/7743461522282941752/{digest}/channel/archive"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
        'keep_alive':'False'
    }
    

    rq = requests.post(url, data = message.encode("utf-8"), headers=headers)
    res = json.loads(rq.text)
    return res
import re
channels = {'bigproj':[-1,0],'tasnimna':[-1,1],'farsna':[-1,2],'iribnews':[-1,3]}
main_words = ['بورس','اقتصاد','تحریم','دولت','دلار','طلا','کرونا','شاخص بورس','تورم','دانشگاه','سقوط','رشد']
# channels = {'iribnews':[-1,0]}
while(True):
    for channel,msgid in channels.items():
        out = get_channel_data(channel,2,-1)
        #print(out)
        if(out['messages'][0]['message_id'] != msgid[0]):
            channels[channel][0] = out['messages'][0]['message_id']
            out['messages'][0]['index'] = msgid[1]
            out['messages'][0]['metadata'] = [tag.strip("#") for tag in out['messages'][0]['text'].split() if tag.startswith("#")] + out['messages'][0]['frequency_word'] + [word for word in main_words if word in out['messages'][0]['text']]
            out['messages'][0]['hashtags'] = [tag.strip("#") for tag in out['messages'][0]['text'].split() if tag.startswith("#")]
            out['messages'][0]['links'] = re.findall(r'(https?://[^\s]+)', out['messages'][0]['text'])
            if out['messages'][0]['file']['extention']=='jpg':
                out['messages'][0]['images'] = [out['messages'][0]['file']['url']]
            else :
                out['messages'][0]['images'] = []
            # print(out['messages'][0])
            # print(' ')
            #publish_message(kafka_producer, 'test', 'raw', out['messages'][0])
            with topic.get_sync_producer() as producer:
                producer.produce(bytes(str(out['messages'][0]), encoding='utf-8'))
    sleep(180)
