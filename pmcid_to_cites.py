import ast
import arrow
import codeswitch
import redis
import requests
import threading
import time
from datetime import timedelta
from edit_queue import EditQueue
from citation_grapher import CitationGrapher
from site_credentials import *

print('Setting up globals')

WRITE_THREAD_COUNT = 2
READ_THREAD_COUNT = 5
THREAD_LIMIT = WRITE_THREAD_COUNT + READ_THREAD_COUNT + 2

# Go from newest Wikidata QID to oldest?
DESCENDING_ORDER = True

eq = EditQueue(
         source='Q229883',
         url_pattern='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pmc&linkname=pmc_refs_pubmed&retmode=json&id=',
         write_thread_count=WRITE_THREAD_COUNT,
         append_value=['P2860'],
         good_refs=[{'P248': None, 'P813': None, 'P854': None}],
         edit_summary='Updating citation graph')

REDIS = redis.Redis(host=redis_server, port=redis_port, password=redis_key)

pmc_template = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"

thread_counter = 0

print('Done setting up globals')

def create_manifest_entry(wikidata_item, pmcid, bundle, retrieve_date):
    cites = []
    for cited_id in bundle:
        cited_item = codeswitch.pmid_to_wikidata(cited_id)
        if cited_item is None:
            continue
        if wikidata_item == cited_item:
            continue
        cites.append(cited_item)

    return (pmcid, tuple(cites), retrieve_date)

class UpdateGraphFast(threading.Thread):  # gotta go fast!
    def __init__(self, threadID, name, package):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.package = package

    def run(self):
        CG = CitationGrapher(eq)
        CG.process_manifest(self.package)
        print('. ', end='')

class UpdateGraph(threading.Thread):
    def __init__ (self, threadID, name, package):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.package = package

    def run(self):
        payload = {
            'dbfrom': 'pmc',
            'linkname': 'pmc_refs_pubmed',
            'tool': 'wikidata_worker',
            'email': 'jamesmhare@gmail.com',
            'retmode': 'json',
            'id': list(self.package.values())}

        post_status = False

        while post_status is False:
            try:
                r = requests.post(pmc_template, data=payload)
                post_status = True
            except requests.exceptions.ConnectionError:
                print('Connection error in ' + self.name + ', trying again in five minutes.')
                time.sleep(300)

        if r.status_code != 200:
            time.sleep(120)
            r = requests.post(pmc_template, data=payload)

            if r.status_code != 200:
                time.sleep(300)
                r = requests.post(pmc_template, data=payload)

        now = arrow.utcnow()
        retrieve_date = '+' + now.format('YYYY-MM-DD') + 'T00:00:00Z'

        try:
            blob = r.json()
        except Exception as e:
            print('ERROR: ' + str(r.status_code))
            return

        # Construct dataset
        manifest = {}  # dict {item: tuple}
        for result in blob["linksets"]:
            relevant_pmcid = result["ids"][0]
            relevant_item = codeswitch.pmcid_to_wikidata(relevant_pmcid)
            if relevant_item is None:
                continue
            REDIS.setex(
                'pmccite_ret:' + str(relevant_pmcid),
                retrieve_date,
                timedelta(days=28))
            if 'linksetdbs' not in result:
                REDIS.setex(
                    'pmccite:' + str(relevant_pmcid),
                    [],
                    timedelta(days=28))
                continue

            REDIS.setex(
                'pmccite:' + str(relevant_pmcid),
                result["linksetdbs"][0]["links"],
                timedelta(days=28))
            add_to_manifest = create_manifest_entry(
                relevant_item,
                relevant_pmcid,
                result["linksetdbs"][0]["links"],
                retrieve_date)
            manifest[relevant_item] = add_to_manifest

        if len(manifest) > 0:
            CG = CitationGrapher(eq)
            CG.process_manifest(manifest)
            print('Processed ' + str(len(manifest)) + ' entries')

def start_thread(thread):
    global thread_counter
    while threading.active_count() >= THREAD_LIMIT:
        time.sleep(0.25)
    thread.start()
    thread_counter += 1
    if thread_counter > 0 and thread_counter % 50 == 0:
        print("Number of remaining edits: " + str(eq.editqueue.qsize()))
    time.sleep(0.25)

def main():
    # First, work off of the Redis cache.
    # Lookups that have been cached go to the "fast track"
    # Otherwise, send to the "slow track"

    slowtrack = {}
    fasttrack = {}

    # Iterating through PMCIDs and assigning to fast track or slow track
    for item, pmcid in codeswitch.get_all_items('P932').items():
        lookup = REDIS.get('pmccite:' + str(pmcid))
        lookup_retrieve_date = REDIS.get('pmccite_ret:' + str(pmcid))

        if lookup is None or lookup_retrieve_date is None:
            slowtrack[item] = pmcid
            if len(slowtrack) >= 50:
                start_thread(UpdateGraph(thread_counter, "thread-" + str(thread_counter), slowtrack))
                slowtrack = {}

        else:
            bundle = ast.literal_eval(lookup.decode('UTF-8'))
            bundle = [int(x) for x in bundle]
            retrieve_date = lookup_retrieve_date.decode('UTF-8')
            fasttrack[item] = create_manifest_entry(item, pmcid, bundle, retrieve_date)
            if len(fasttrack) >= 50:
                start_thread(UpdateGraphFast(thread_counter, "thread-" + str(thread_counter), fasttrack))
                fasttrack = {}

    # If there are leftovers
    if len(fasttrack) > 0:
        start_thread(UpdateGraphFast(thread_counter, "thread-" + str(thread_counter), fasttrack))

    if len(slowtrack) > 0:
        start_thread(UpdateGraph(thread_counter, "thread-" + str(thread_counter), slowtrack))

    eq.done()  # Tell the editor threads they can stop now

    print("Number of remaining edits: " + str(eq.editqueue.qsize()))

if __name__ == '__main__':
    main()
