import arrow
import math
import redis
import requests
import threading
import time
from site_credentials import *

manifest = ['P356', 'P698', 'P932']
read_threads = 9

REDIS = redis.Redis(host=redis_server, port=redis_port, password=redis_key)
THREAD_LIMIT = 2 + len(manifest) + read_threads
today = arrow.utcnow().format('YYYYMMDD')
limit = {}

def process_blob(prop, blob):
    if '@graph' not in blob:
        return False

    for entry in blob['@graph']:
        if prop not in entry:
            continue

        wd_item = entry['@id'].replace('wd:', '')
        prop_value = entry[prop]

        REDIS.hset(
            '{0}_to_wikidata_{1}'.format(prop, today),
            prop_value,
            wd_item)
        REDIS.hset('wikidata_to_{0}_{1}'.format(prop, today),
            wd_item,
            prop_value)

def get_blob(prop, counter):
    url = ('https://query.wikidata.org/bigdata/ldf?subject=&predicate='
           'http%3A%2F%2Fwww.wikidata.org%2Fprop%2Fdirect%2F{0}&object='
           '&page={1}')

    print(prop, '-', str(counter))

    while True:
        r = requests.get(
                url.format(prop, counter),
                headers={"Accept": "application/ld+json"})
        if 'Rate limit exceeded' in r.text:
            time.sleep(10)
        else:
            break

    try:
        return r.json()
    except:
        print(r.text)

class GetAndProcessBlob(threading.Thread):
    def __init__(self, threadID, name, prop, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.prop = prop
        self.counter = counter

    def run(self):
        prop = self.prop
        counter = self.counter

        if prop in limit:
            if counter > limit[prop]:
                return False

        blob = get_blob(prop, counter)
        process_blob(prop, blob)

class CreateCache(threading.Thread):  # gotta go fast!
    def __init__(self, threadID, name, prop):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.prop = prop

    def run(self):
        global limit
        prop = self.prop

        blob = get_blob(prop, 1)
        process_blob(prop, blob)

        for entry in blob['@graph']:
            if 'void:triples' in entry:
                limit[prop] = int(math.ceil(entry['void:triples'] / 100))

        # At this point, if `limit[prop]` is not defined, something is seriously
        # wrong.

        if prop not in limit:
            raise Exception

        counter = 2

        # Time to iterate through the rest of the pages up to the limit
        while True:
            if counter > limit[prop]:
                break

            while threading.active_count() >= THREAD_LIMIT:
                time.sleep(0.25)

            thread = GetAndProcessBlob(
                counter,
                'thread-' + prop + '-' + str(counter),
                prop,
                counter)

            thread.start()
            counter += 1

        REDIS.rename(
            '{0}_to_wikidata_{1}'.format(prop, today),
            '{0}_to_wikidata'.format(prop))
        REDIS.rename(
            'wikidata_to_{0}_{1}'.format(prop, today),
            'wikidata_to_{0}'.format(prop))

def main(manifest):
    thread_counter = 0
    for prop in manifest:
        thread = CreateCache(
            thread_counter,
            'thread-' + str(thread_counter),
            prop)
        thread.start()
        thread_counter += 1

if __name__ == '__main__':
    main(manifest)
