import ast
import arrow
import redis
import requests
import threading
import time
from BiblioWikidata import JournalArticles
from collections import Counter
from datetime import timedelta
from mem_top import mem_top
from edit_queue import EditQueue
from citation_grapher import CitationGrapher

print('Setting up globals')  # debug

WRITE_THREAD_COUNT = 3
READ_THREAD_COUNT = 1
THREAD_LIMIT = WRITE_THREAD_COUNT + READ_THREAD_COUNT + 2

eq = EditQueue(
         source=self.source,
         url_pattern=self.url_pattern,
         write_thread_count=self.write_thread_count,
         append_value=['P2860'],
         good_refs=[{'P248': None, 'P813': None, 'P854': None}],
         edit_summary='Updating citation graph')

REDIS = redis.Redis(host='127.0.0.1', port=6379)

pmc_template = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"
pmcid_seed = "https://query.wikidata.org/sparql?query=select%20%3Fitem%20%3Fpmcid%20where%20%7B%20%3Fitem%20wdt%3AP932%20%3Fpmcid%20%7D"

pmcid_list = []  # list of strings: "wikidata item|pmcid"

r = requests.get(pmcid_seed, stream=True, headers={'Accept': 'text/tab-separated-values'})
with open('/tmp/pmcid2wikidata.tsv', 'wb') as f:
    for chunk in r.iter_content(chunk_size=1024):
        f.write(chunk)

with open('/tmp/pmcid2wikidata.tsv') as f:
    for line in f:
        line = line.replace('\0', '').split('\t')
        if len(line) < 2:
            continue
        item = line[0].replace('<http://www.wikidata.org/entity/', '').replace('>', '').strip()
        pmcid = line[1].strip()
        pmcid_list.append(item + '|' + pmcid)
        REDIS.hset('pmcid_to_wikidata', pmcid, item)

pmcid_list = list(set(pmcid_list))  # Removing duplicates

# Keeping only the PMCIDs while sorting in order of the Wikidata IDs
pmcid_list.sort(reverse=True)
pmcid_list = [int(x.split('|')[1]) for x in pmcid_list if x != '?item|?pmcid']
pmcid_list = tuple(pmcid_list)

pmid_seed = "https://query.wikidata.org/sparql?query=SELECT%20%3Fitem%20%3Fpmid%20WHERE%20%7B%0A%20%20%3Fitem%20wdt%3AP698%20%3Fpmid%20.%0A%7D"

r = requests.get(pmid_seed, stream=True, headers={'Accept': 'text/tab-separated-values'})
with open('/tmp/pmid2wikidata.tsv', 'wb') as f:
    for chunk in r.iter_content(chunk_size=1024):
        f.write(chunk)

package = {}
with open('/tmp/pmid2wikidata.tsv') as f:
    for linenum, line in enumerate(f):
        line = line.replace('\0', '').split('\t')
        if len(line) < 2:
            continue
        if str(line[1].strip()) == '?pmid':
            continue
        try:
            pmid = int(line[1].strip())
        except ValueError:
            continue
        item = line[0].replace('<http://www.wikidata.org/entity/', '').replace('>', '').strip()
        package[pmid] = item
        if linenum > 0 and linenum % 50 == 0:
            REDIS.hmset(
                'pmid_to_wikidata',
                 package)
            package = {}

    # Save the last package
    REDIS.hmset(
        'pmid_to_wikidata',
         package)

del package

nonexistent_pmid = Counter()

print('Done setting up globals')  # debug

def create_manifest_entry(wikidata_item, pmcid, bundle, retrieve_date):
    cites = []
    for cited_id in bundle:
        cited_item = REDIS.hget('pmid_to_wikidata', cited_id)
        if cited_item is None:
            # Disabled for performance
            #nonexistent_pmid[cited_id] += 1
            continue
        else:
            cited_item = cited_item.decode('utf-8')
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
		    CG = CitationGrapher(
                 'Q229883',
                 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pmc&linkname=pmc_refs_pubmed&retmode=json&id=',
		             eq,
                 write_thread_count=WRITE_THREAD_COUNT)
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
            relevant_item = REDIS.hget('pmcid_to_wikidata', relevant_pmcid)
            if relevant_item is None:
                continue
            relevant_item = relevant_item.decode('utf-8')
            REDIS.setex(
                'pmccite_ret:' + str(relevant_pmcid),
                retrieve_date,
                timedelta(days=14))
            if 'linksetdbs' not in result:
                REDIS.setex(
                    'pmccite:' + str(relevant_pmcid),
                    [],
                    timedelta(days=14))
                continue

            REDIS.setex(
                'pmccite:' + str(relevant_pmcid),
                result["linksetdbs"][0]["links"],
                timedelta(days=14))
            add_to_manifest = create_manifest_entry(
                relevant_item,
                relevant_pmcid,
                result["linksetdbs"][0]["links"],
                retrieve_date)
            manifest[relevant_item] = add_to_manifest

        if len(manifest) > 0:
            CG = CitationGrapher(
                     'Q229883',
                     'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pmc&linkname=pmc_refs_pubmed&retmode=json&id=',
	                   eq,
                     write_thread_count=WRITE_THREAD_COUNT)
            CG.process_manifest(manifest)
            print('Processed ' + str(len(manifest)) + ' entries')

def main():
    # First, work off of the Redis cache.
    # Lookups that have been cached go to the "fast track"
    # Otherwise, send to the "slow track"

    slowtrack = {}
    fasttrack = {}
    thread_counter = 0

    for pmcid in pmcid_list:

        item = REDIS.hget('pmcid_to_wikidata', pmcid)
        if item is None:
            continue

        lookup = REDIS.get('pmccite:' + str(pmcid))
        lookup_retrieve_date = REDIS.get('pmccite_ret:' + str(pmcid))

        if lookup is None or lookup_retrieve_date is None:
            slowtrack[item] = pmcid
            if len(slowtrack) >= 50:
                thread = UpdateGraph(thread_counter, "thread-" + str(thread_counter), slowtrack)
                thread_counter += 1
                while threading.active_count() >= THREAD_LIMIT:
                    time.sleep(1)
                thread.start()
                time.sleep(1)
                slowtrack = {}
                if thread_counter > 0 and thread_counter % 50 == 0:
                    print("Number of remaining edits: " + str(CG.eq.editqueue.qsize()))
                    print('As of thread ' + str(thread_counter) + ':\n')
                    print(mem_top())  # debug

        else:
            bundle = ast.literal_eval(lookup.decode('UTF-8'))
            bundle = [int(x) for x in bundle]
            retrieve_date = lookup_retrieve_date.decode('UTF-8')
            fasttrack[item.decode('utf-8')] = create_manifest_entry(item.decode('utf-8'), pmcid, bundle, retrieve_date)
            if len(fasttrack) >= 50:
                thread = UpdateGraphFast(thread_counter, "thread-" + str(thread_counter), fasttrack)
                thread_counter += 1
                while threading.active_count() >= THREAD_LIMIT:
                    time.sleep(1)
                thread.start()
                fasttrack = {}
                if thread_counter > 0 and thread_counter % 50 == 0:
                    print("Number of remaining edits: " + str(CG.eq.editqueue.qsize()))
                    print('As of thread ' + str(thread_counter) + ':\n')
                    print(mem_top())  # debug

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
                print('As of thread ' + str(thread_counter) + ':\n')
                print(mem_top())  # debug

        print('\nProcessed ' + str(len(fasttrack)) + ' cached entries')

    packages = [slowtrack[x:x+50] for x in range(0, len(slowtrack), 50)]

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

    #for identifier, counter in nonexistent_pmid.items():
    #    if counter >= 500:
    #        JournalArticles.item_creator([{'pmid': identifier}])

if __name__ == '__main__':
    main()
