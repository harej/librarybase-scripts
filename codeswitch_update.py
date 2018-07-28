import arrow
import redis
import requests
import threading
from site_credentials import *

REDIS = redis.Redis(host=redis_server, port=redis_port, password=redis_key)
today = arrow.utcnow().format('YYYYMMDD')

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

def get_and_process_blob(prop, counter):
    url = ('https://query.wikidata.org/bigdata/ldf?subject=&predicate='
           'http%3A%2F%2Fwww.wikidata.org%2Fprop%2Fdirect%2F{0}&object='
           '&page={1}')

    print(prop, '-', str(counter))
    r = requests.get(
        url.format(prop, counter),
        headers={"Accept": "application/ld+json"})

    try:
        blob = r.json()
    except:
        # If it doesn't work, most likely the server returned a 500 error,
        # which it does in HTML for some reason. In any case, we're done.
        return False

    # Processing blob. A return of False means that despite being valid
    # JSON, the content we want is not there.
    if process_blob(prop, blob) is False:
        return False


class CreateCache(threading.Thread):  # gotta go fast!
    def __init__(self, threadID, name, prop):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.prop = prop

    def run(self):
        prop = self.prop
        counter = 1

        while True:
            if get_and_process_blob(prop, counter) is False:
                break
            counter += 1

        REDIS.rename(
            '{0}_to_wikidata_{1}'.format(prop, today),
            '{0}_to_wikidata'.format(prop))
        REDIS.rename(
            'wikidata_to_{0}_{1}'.format(prop, today),
            'wikidata_to_{0}'.format(prop))

def main():
    thread_counter = 0
    for prop in ['P356', 'P698', 'P932']:
        thread = CreateCache(
            thread_counter,
            'thread-' + str(thread_counter),
            prop)
        thread.start()
        thread_counter += 1

if __name__ == '__main__':
    main()
