import citation_grapher
import ast
import arrow
import redis
import requests
import threading
import time
from BiblioWikidata import JournalArticles
from datetime import timedelta

CG = citation_grapher.CitationGrapher('Q5188229', 'https://api.crossref.org/works/')
REDIS = redis.Redis(host='127.0.0.1', port=6379)
s = requests.Session()

doi_to_wikidata = {}  # for pairing DOIs to Wikidata items
doi_list = []  # to do look-ups with
nonexistent_doi = {}  # counter of how many times a given DOI is cited
doi_seed = 'SELECT%20%3Fitem%20%3Fdoi%20WHERE%20%7B%20%3Fitem%20wdt%3AP356%20%3Fdoi%20%7D'
q = requests.get('https://query.wikidata.org/sparql?format=json&query=' + doi_seed).json()['results']['bindings']

for entry in q:
    wikidata_item = entry['item']['value'].replace('http://www.wikidata.org/entity/', '')
    the_doi = entry['doi']['value']
    doi_to_wikidata[the_doi] = wikidata_item
    doi_list.append((wikidata_item, the_doi))

doi_list = sorted(doi_list, reverse=True)

def create_manifest_entry(wikidata_item, doi, bundle, retrieve_date):
    add_to_manifest = {
        'wikidata': wikidata_item,
        'external_id': doi,
        'cites': [],
        'retrieve_date': retrieve_date}
    if bundle == []:
        return add_to_manifest

    if 'message' in bundle:
        if 'reference' in bundle['message']:
            for block in bundle['message']['reference']:
                if 'DOI' not in block:
                    continue
                if block['DOI'].upper() not in doi_to_wikidata:
                    if block['DOI'].upper() in nonexistent_doi:
                        nonexistent_doi[block['DOI'].upper()] += 1
                    else:
                        nonexistent_doi[block['DOI'].upper()] = 1
                    continue
                cited_item = doi_to_wikidata[block['DOI'].upper()]
                if wikidata_item == cited_item:
                    continue
                add_to_manifest['cites'].append(cited_item)

    return add_to_manifest

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
    def __init__ (self, threadID, name, entry):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.entry = entry

    def run(self):
        wikidata_item = self.entry[0]
        relevant_doi = self.entry[1]
        found_data = False

        now = arrow.utcnow()
        retrieve_date = '+' + now.format('YYYY-MM-DD') + 'T00:00:00Z'

        r = s.get('https://api.crossref.org/works/' + relevant_doi)

        REDIS.setex(
            'doi_to_cites__' + relevant_doi + '_retrieve_date',
            retrieve_date,
            timedelta(days=30))

        if r.status_code == 404:
            REDIS.setex(
                'doi_to_cites__' + relevant_doi,
                {},
                timedelta(days=30))
            return

        if r.status_code != 200:
            time.sleep(120)
            r = s.get('https://api.crossref.org/works/' + relevant_doi)

            if r.status_code != 200:
                time.sleep(300)
                r = s.get('https://api.crossref.org/works/' + relevant_doi)

        try:
            blob = r.json()
        except Exception as e:
            print('ERROR: ' + str(r.status_code))
            return

        if 'message' in blob:
            if 'reference' in blob['message']:
                found_data = True
                relevant_item = doi_to_wikidata[relevant_doi]
                REDIS.setex(
                    'doi_to_cites__' + relevant_doi,
                    blob,
                    timedelta(days=14))
                add_to_manifest = create_manifest_entry(
                    relevant_item,
                    relevant_doi,
                    blob,
                    retrieve_date)
                CG.process_manifest([add_to_manifest])

        if found_data is False:
            REDIS.setex(
                'doi_to_cites__' + relevant_doi,
                {},
                timedelta(days=14))

def main():
    # First, work off of the Redis cache.
    # Lookups that have been cached go to the "fast track"
    # Otherwise, send to the "slow track"

    slowtrack = []
    fasttrack = []
    threads = []
    for pair in doi_list:
        lookup = REDIS.get('doi_to_cites__' + pair[1])
        lookup_retrieve_date = REDIS.get('doi_to_cites__' + pair[1] + '_retrieve_date')
        if lookup is None or lookup_retrieve_date is None:
            slowtrack.append(pair)
        else:
            bundle = ast.literal_eval(lookup.decode('UTF-8'))
            retrieve_date = lookup_retrieve_date.decode('UTF-8')
            fasttrack.append(create_manifest_entry(pair[0], pair[1], bundle, retrieve_date))

    if len(fasttrack) > 0:
        thread_counter = 0
        for package in [fasttrack[x:x+500] for x in range(0, len(fasttrack), 500)]:
            threads.append(UpdateGraphFast(thread_counter, "thread-" + str(thread_counter), package))
            thread_counter += 1
        for thread in threads:
            thread.start()
            time.sleep(1)
        for thread in threads:
            thread.join()
        print('\nProcessed ' + str(len(fasttrack)) + ' cached entries')

    threads = []
    thread_counter = 0
    for entry in slowtrack:
        threads.append(UpdateGraph(thread_counter, "thread-" + str(thread_counter), entry))
        thread_counter += 1
    for thread in threads:
        thread.start()
        time.sleep(.2)
    for thread in threads:
        thread.join()

    CG.event.set()  # Tell the editor threads they can stop now

    print("Number of remaining edits: " + str(CG.editqueue.qsize()))

    for doi, counter in nonexistent_doi.items():
        if counter >= 50:
            JournalArticles.item_creator([{'doi': doi}])

if __name__ == '__main__':
    main()
