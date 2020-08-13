from clickhouse_driver import Client
client = Client(host='localhost')

client.execute('CREATE DATABASE IF NOT EXISTS clickhouse')
client.execute("""CREATE TABLE clickhouse.all_posts(
    channel_id String
    ,eventtime DateTime,message_id String ,hashtags Array(String)
    ,metadata Array(String)
    )
    ENGINE = Memory
    """)