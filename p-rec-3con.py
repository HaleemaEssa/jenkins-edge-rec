#!/usr/bin/env python
import pika, sys, os
import datetime
from datetime import datetime
import time
import csv
import pandas
def main():
    with open('/data/data_con.csv', mode='w') as data:
        data_writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    import pandas as pd
    f = open("/data/data_con.csv","a")
    if os.stat('/data/data_con.csv').st_size == 0:
        f.write("Date,Sound,Flame,Humidity,Temperature\n")
    f = open('/data/data_con.csv', 'r+')
    credentials = pika.PlainCredentials('haleema', '4chyst')
    parameters = pika.ConnectionParameters('192.168.0.126',
                                   5672,
                                   '/',
                                   credentials)

    connection = pika.BlockingConnection(parameters)
    #connection_f = pika.BlockingConnection(parameters)
    #connection_dht = pika.BlockingConnection(parameters)


    channel_s = connection.channel()
    channel_s.exchange_declare(exchange='logs_sound', exchange_type='fanout')
    result_s = channel_s.queue_declare(queue='', exclusive=True)
    queue_name = result_s.method.queue
    channel_s.queue_bind(exchange='logs_sound', queue=queue_name)

    channel_f = connection.channel()
    channel_f.exchange_declare(exchange='logs_flame', exchange_type='fanout')
    result_f = channel_f.queue_declare(queue='', exclusive=True)
    queue_name = result_f.method.queue
    channel_f.queue_bind(exchange='logs_flame', queue=queue_name)
    
    channel_dht = connection.channel()
    channel_dht.exchange_declare(exchange='logs_dht', exchange_type='fanout')
    result_dht = channel_dht.queue_declare(queue='', exclusive=True)
    queue_name = result_dht.method.queue
    channel_dht.queue_bind(exchange='logs_dht', queue=queue_name)
    
    def callback_s(ch, method, properties, body): #for tem
            f = open("/data/data_con.csv","a")
            print(datetime.today().strftime('%Y/%m/%d %H:%M:%S') + ":   received    "+ str( body)+"\n")
            # m= body.decode()                     
            f.write(datetime.today().strftime('%Y-%m-%d %H:%M:%S')+","+str(body))
    channel_s.basic_consume(queue=queue_name, on_message_callback=callback_s, auto_ack=True)

    print(' [*] Waiting for data. To exit press CTRL+C')
    channel_s.start_consuming()
    
    def callback_f(ch, method, properties, body): #for tem
            f = open("/data/data_con.csv","a")
            print(datetime.today().strftime('%Y/%m/%d %H:%M:%S') + ":   received    "+ str( body)+"\n")
            f.write(","+str(body))
    channel_f.basic_consume(queue=queue_name, on_message_callback=callback_f, auto_ack=True)
    #print(' [*] Waiting for flame data. To exit press CTRL+C')
    channel_f.start_consuming()
    
    def callback_dht(ch, method, properties, body): #for tem
            f = open("/data/data_con.csv","a")
            print(datetime.today().strftime('%Y/%m/%d %H:%M:%S') + ":   received    "+ str( body)+"\n")
            f.write(","+str(body))
    channel_dht.basic_consume(queue=queue_name, on_message_callback=callback_dht, auto_ack=True)
    #print(' [*] Waiting for dht data. To exit press CTRL+C')
    channel_dht.start_consuming()
    
if __name__ == '__main__':
    try:
        main()
    except SystemExit:
        os._exit(0)    

