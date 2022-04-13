#!/usr/bin/env python
import pika, sys, os
import datetime
from datetime import datetime
import time
import csv
import pandas
def main():
    with open('/data/data-con.csv', mode='w') as data:
        data_writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    import pandas as pd
    f = open("/data/data-con.csv","a")
    if os.stat('/data/data-con.csv').st_size == 0:
        f.write("Date,Sound,Flame,Humidity,Temperature\n")
    f = open('/data/data-con.csv', 'r+')
    credentials = pika.PlainCredentials('haleema', '4chyst')
    parameters = pika.ConnectionParameters('192.168.0.126',
                                   5672,
                                   '/',
                                   credentials)

    connection = pika.BlockingConnection(parameters)

    channel-s = connection.channel()
    channel-s.exchange_declare(exchange='logs-sound', exchange_type='fanout')
    result-s = channel-s.queue_declare(queue='', exclusive=True)
    queue_name = result-s.method.queue
    channel-s.queue_bind(exchange='logs-sound', queue=queue_name)

    channel-f = connection.channel()
    channel-f.exchange_declare(exchange='logs-flame', exchange_type='fanout')
    result-f = channel-f.queue_declare(queue='', exclusive=True)
    queue_name = result-f.method.queue
    channel-f.queue_bind(exchange='logs-flame', queue=queue_name)
    
    channel-dht = connection.channel()
    channel-dht.exchange_declare(exchange='logs-dht', exchange_type='fanout')
    result-dht = channel-dht.queue_declare(queue='', exclusive=True)
    queue_name = result-dht.method.queue
    channel-dht.queue_bind(exchange='logs-dht', queue=queue_name)
    
    def callback-s(ch, method, properties, body): #for tem
            f = open("/data/data-con.csv","a")
            print(datetime.today().strftime('%Y/%m/%d %H:%M:%S') + ":   received    "+ str( body)+"\n")
            # m= body.decode()                     
            f.write(datetime.today().strftime('%Y-%m-%d %H:%M:%S')+","+str(body))
    channel-s.basic_consume(queue=queue_name, on_message_callback=callback-s, auto_ack=True)

    print(' [*] Waiting for sound data. To exit press CTRL+C')
    channel-s.start_consuming()
    
    def callback-f(ch, method, properties, body): #for tem
            f = open("/data/data-con.csv","a")
            print(datetime.today().strftime('%Y/%m/%d %H:%M:%S') + ":   received    "+ str( body)+"\n")
            f.write(","+str(body))
    channel-f.basic_consume(queue=queue_name, on_message_callback=callback-f, auto_ack=True)
    print(' [*] Waiting for flame data. To exit press CTRL+C')
    channel-f.start_consuming()
    
    def callback-dht(ch, method, properties, body): #for tem
            f = open("/data/data-con.csv","a")
            print(datetime.today().strftime('%Y/%m/%d %H:%M:%S') + ":   received    "+ str( body)+"\n")
            f.write(","+str(body))
    channel-dht.basic_consume(queue=queue_name, on_message_callback=callback-dht, auto_ack=True)
    print(' [*] Waiting for dht data. To exit press CTRL+C')
    channel-dht.start_consuming()
    
if __name__ == '__main__':
    try:
        main()
    except SystemExit:
        os._exit(0)    

