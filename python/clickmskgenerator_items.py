# haimtran on 22/07/2023
# generate click stream to kafka topic 

from kafka import KafkaProducer
import json, time, random
from datetime import datetime
import sys
import time
import hashlib

def get_event_id():
    hashed = hashlib.md5(datetime.now().strftime("%m/%d/%YT%H:%M:%S.%f").encode())

    return hashed.hexdigest()

def get_event():
    events = [
        "purchased_item", "liked_item", "reviewed_item", "entered_payment_method",
        "clicked_review", "clicked_item_description"
    ]

    return random.choice(events)

def get_item_quantity(eventname):
    MAX_ITEM_LIMIT = 5
    if (eventname == 'purchased_item'):
        itemqty=random.randint(1, MAX_ITEM_LIMIT)
    else:
        itemqty=0
    return itemqty

def get_item_id(page_name):
    #print(page_name)
    if (page_name == 'apparel'):
        MIN_ITEM_LIMIT = 11
        MAX_ITEM_LIMIT = 13
    elif (page_name == 'food'):
        MIN_ITEM_LIMIT = 21
        MAX_ITEM_LIMIT = 23
    elif (page_name == 'electronics'):
        MIN_ITEM_LIMIT = 31
        MAX_ITEM_LIMIT = 33
    elif (page_name == 'home'):
        MIN_ITEM_LIMIT = 41
        MAX_ITEM_LIMIT = 43
    else:
        MIN_ITEM_LIMIT = 51
        MAX_ITEM_LIMIT = 53

    #print(MIN_ITEM_LIMIT)
    #print(MAX_ITEM_LIMIT)
    return random.randint(MIN_ITEM_LIMIT, MAX_ITEM_LIMIT)

def get_user_id():
    MAX_USER_ID = 50

    return random.randint(1, MAX_USER_ID)


def get_sales():
    MAX_SALES = 50000

    return random.randint(5000, MAX_SALES)

def get_event_time():
    #return datetime.now().strftime("%m/%d/%YT%H:%M:%S.%f")
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

def get_os():
    os = ["ios", "android", "web"]

    return random.choice(os)

def get_page():
    pages = ["apparel", "food", "electronics", "home", "books"]

    return random.choice(pages)

#def get_stock(): return { 'event_time': datetime.now().isoformat(), 'ticker': random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV']), 'price': round(random.ndom.random() * 100, 2) }

if __name__ == '__main__':
    print ("\n Number of arguments:", len(sys.argv), "arguments")
    print ("\n Argument List:", str(sys.argv))
    print ("\n The program name is:", str(sys.argv[0]))
    print ("\n The bootstrap server list :", str(sys.argv[1]))
    print ("\n The msk topic name :", str(sys.argv[2]))
    print ("\n Max interval in seconds between records :", str(sys.argv[3]))
    BOOTSTRAP_SERVERS=str(sys.argv[1])
    #print(BOOTSTRAP_SERVERS)
    TOPIC=str(sys.argv[2])
    MAX_SECONDS_BETWEEN_EVENTS = int(sys.argv[3])

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=str.encode,
        retry_backoff_ms=500,
        request_timeout_ms=20000,
        security_protocol='SSL')

    while True:
        #data = get_stock()
        #print(data)
        delay = random.randint(0, MAX_SECONDS_BETWEEN_EVENTS)
        time.sleep(delay)
        eventname=get_event()
        pagename=get_page()

        #if eventname == 'purchased_item':
        itemid=get_item_id(pagename)
        itemquantity=get_item_quantity(eventname)
        #else:
            #itemid=0
            #itemquantity=0

        event = {
            'event_id': get_event_id(),
            'event': eventname,
            'user_id': get_user_id(),
            'item_id': itemid,
            'item_quantity': itemquantity,
            'event_time': get_event_time(),
            'os': get_os(),
            'page': pagename,
            'url': 'www.example.com'
           }
                  #data = json.dumps(event)
        print(event)
        try:
            #print('\n In the try block')
            future = producer.send(TOPIC, value=event, key=event['page'])
            producer.flush()

            record_metadata = future.get(timeout=10)
            print('sent event to Kafka! topic {} partition {} offset {}\n'.format(record_metadata.topic, record_metadata.partition,record_metadata.offset))
        except Exception as e:
            print(e.with_traceback())