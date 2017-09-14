import citation_grapher
import ast
import arrow
import redis
import requests
import threading
import time
from BiblioWikidata import JournalArticles
from collections import OrderedDict
from datetime import timedelta
from mem_top import mem_top

print('Setting up globals')  # debug

WRITE_THREAD_COUNT = 2
READ_THREAD_COUNT = 4
THREAD_LIMIT = WRITE_THREAD_COUNT + READ_THREAD_COUNT + 2

CG = citation_grapher.CitationGrapher(
    'Q229883',
    'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pmc&linkname=pmc_refs_pubmed&retmode=json&id=',
    write_thread_count=WRITE_THREAD_COUNT)


REDIS = redis.Redis(host='127.0.0.1', port=6379)
pmc_template = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"
pmcid_seed = "https://query.wikidata.org/sparql?format=json&query=select%20%3Fitem%20%3Fpmcid%20where%20%7B%20%3Fitem%20wdt%3AP932%20%3Fpmcid%20%7D"

pmcid_list = []  # list of tuples: (wikidata item, PMCID)

for x in requests.get(pmcid_seed).json()["results"]["bindings"]:
    item = x["item"]["value"].replace("http://www.wikidata.org/entity/", "")
    pmcid = x["pmcid"]["value"]
    pmcid_list.append((item, pmcid))

pmcid_list = list(set(pmcid_list))  # Removing duplicates
pmcid_list.sort(reverse=True)

pmcid_to_wikidata = OrderedDict()

for pair in pmcid_list:
    item = pair[0]
    pmcid = pair[1]
    pmcid_to_wikidata[pmcid] = item

del pmcid_list

pmid_seed = "https://query.wikidata.org/sparql?format=json&query=SELECT%20%3Fitem%20%3Fpmid%20WHERE%20%7B%0A%20%20%3Fitem%20wdt%3AP698%20%3Fpmid%20.%0A%7D"
pmid_to_wikidata = {x["pmid"]["value"]: x["item"]["value"].replace("http://www.wikidata.org/entity/", "") \
                    for x in requests.get(pmid_seed).json()["results"]["bindings"]}

nonexistent_pmid = {}

print('Done setting up globals')  # debug

def create_manifest_entry(wikidata_item, pmcid, bundle, retrieve_date):
    cites = []
    for cited_id in bundle:
        if str(cited_id) not in pmid_to_wikidata:
            if str(cited_id) in nonexistent_pmid:
                nonexistent_pmid[str(cited_id)] += 1
            else:
                nonexistent_pmid[str(cited_id)] = 1
            continue
        cited_item = pmid_to_wikidata[str(cited_id)]
        if wikidata_item == cited_item:
            continue
        cites.append(cited_item)

    return (wikidata_item, pmcid, cites, retrieve_date)

class UpdateGraphFast(threading.Thread):  # gotta go fast!
    def __init__(self, threadID, name, package):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.package = package

    def run(self):
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
            'id': [x[1] for x in self.package]}

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
        manifest = []  # list of tuples
        for result in blob["linksets"]:
            if str(result["ids"][0]) not in pmcid_to_wikidata:
                continue
            relevant_pmcid = str(result["ids"][0])
            REDIS.setex(
                'pmcid_to_cites__' + relevant_pmcid + '_retrieve_date',
                retrieve_date,
                timedelta(days=14))
            if 'linksetdbs' not in result:
                REDIS.setex(
                    'pmcid_to_cites__' + relevant_pmcid,
                    [],
                    timedelta(days=14))
                continue
            relevant_item = pmcid_to_wikidata[relevant_pmcid]

            REDIS.setex(
                'pmcid_to_cites__' + relevant_pmcid,
                result["linksetdbs"][0]["links"],
                timedelta(days=14))
            add_to_manifest = create_manifest_entry(
                relevant_item,
                relevant_pmcid,
                result["linksetdbs"][0]["links"],
                retrieve_date)
            manifest.append(add_to_manifest)

        if len(manifest) > 0:
            CG.process_manifest(manifest)
            print('Processed ' + manifest[0][0] + ' through ' + manifest[len(manifest) - 1][0])

def main():
    # First, work off of the Redis cache.s
    # Lookups that have been cached go to the "fast track"
    # Otherwise, send to the "slow track"

    slowtrack = []
    fasttrack = []
    thread_counter = 0

    for pmcid, item in pmcid_to_wikidata.items():

        lookup = REDIS.get('pmcid_to_cites__' + pmcid)
        lookup_retrieve_date = REDIS.get('pmcid_to_cites__' + pmcid + '_retrieve_date')

        if lookup is None or lookup_retrieve_date is None:
            slowtrack.append((item, pmcid))
            if len(slowtrack) >= 250:
                thread = UpdateGraph(thread_counter, "thread-" + str(thread_counter), slowtrack)
                thread_counter += 1
                while threading.active_count() >= THREAD_LIMIT:
                    time.sleep(1)
                thread.start()
                time.sleep(1)
                slowtrack = []
                if thread_counter > 0 and thread_counter % 50 == 0:
                    print("Number of remaining edits: " + str(CG.eq.editqueue.qsize()))
                    #print('As of thread ' + str(thread_counter) + ':\n')
                    #print(mem_top())  # debug

        else:
            bundle = ast.literal_eval(lookup.decode('UTF-8'))
            bundle = [str(x) for x in bundle]
            retrieve_date = lookup_retrieve_date.decode('UTF-8')
            fasttrack.append(create_manifest_entry(item, pmcid, bundle, retrieve_date))
            if len(fasttrack) >= 50:
                thread = UpdateGraphFast(thread_counter, "thread-" + str(thread_counter), fasttrack)
                thread_counter += 1
                while threading.active_count() >= THREAD_LIMIT:
                    time.sleep(1)
                thread.start()
                fasttrack = []
                if thread_counter > 0 and thread_counter % 50 == 0:
                    print("Number of remaining edits: " + str(CG.eq.editqueue.qsize()))
                    #print('As of thread ' + str(thread_counter) + ':\n')
                    #print(mem_top())  # debug

    if len(fasttrack) > 0:
        for package in [fasttrack[x:x+50] for x in range(0, len(fasttrack), 50)]:
            thread = UpdateGraphFast(thread_counter, "thread-" + str(thread_counter), package)
            thread_counter += 1
            while threading.active_count() >= THREAD_LIMIT:
                time.sleep(1)
            thread.start()
            time.sleep(1)
            if thread_counter > 0 and thread_counter % 50 == 0:
                print("Number of remaining edits: " + str(CG.eq.editqueue.qsize()))
                #print('As of thread ' + str(thread_counter) + ':\n')
                #print(mem_top())  # debug

        print('\nProcessed ' + str(len(fasttrack)) + ' cached entries')

    packages = [slowtrack[x:x+250] for x in range(0, len(slowtrack), 250)]

    threads = []
    for package in packages:
        threads.append(UpdateGraph(thread_counter, "thread-" + str(thread_counter), package))
        thread_counter += 1
    for thread in threads:
        while threading.active_count() >= THREAD_LIMIT:
            time.sleep(1)
        thread.start()
        time.sleep(1)
    for thread in threads:
        thread.join()

    CG.eq.done()  # Tell the editor threads they can stop now

    print("Number of remaining edits: " + str(CG.eq.editqueue.qsize()))

    for identifier, counter in nonexistent_pmid.items():
        if counter >= 500:
            JournalArticles.item_creator([{'pmid': identifier}])

if __name__ == '__main__':
    main()
