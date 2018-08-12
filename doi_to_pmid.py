import codeswitch
import redis
import requests
import threading
import time
from datetime import timedelta
from site_credentials import *

REDIS = redis.Redis(host=redis_server, port=redis_port, password=redis_key)
THREAD_LIMIT = 9

class AskPubMed(threading.Thread):
    def __init__(self, threadID, name, doi):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.doi = doi

    def run(self):
        doi = self.doi
        wikidata_item = codeswitch.doi_to_wikidata(doi)

        if REDIS.get('doi_to_pmid__' + doi) is not None:
            return

        try:
            r = requests.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?retmode=json&db=pubmed&term=" + doi).json()
        except (OSError, ValueError):
            return

        found = False
        if "esearchresult" in r:
            if "count" in r["esearchresult"]:
                if r["esearchresult"]["count"] == "1":
                    if "errorlist" not in r["esearchresult"]:
                        found = True
                        pmid = r["esearchresult"]["idlist"][0]
                        print(wikidata_item + "\tP698\t\"" + pmid + "\"")
                        REDIS.set('doi_to_pmid__' + doi, pmid)

        if found is False:
            REDIS.setex(
                'doi_to_pmid__' + doi,
                '',
                timedelta(days=14))


def main():
    print('Getting all Wikidata items with P356...')
    wikidata_to_doi = codeswitch.hgetall('wikidata_to_P356')
    print('Done')

    print('Getting all Wikidata items with P698 (so we can filter them out)...')
    wikidata_to_pmid = codeswitch.hgetall('wikidata_to_P698')
    print('Done')

    whitelist = list(wikidata_to_doi.keys())
    blacklist = list(wikidata_to_pmid.keys())

    print('Filtering out the ones we don\'t need to process...')
    whitelist = list(set(whitelist) - set(blacklist))
    dois = []
    for wd_item in whitelist:
        dois.append(wikidata_to_doi[wd_item])
    print('Done')

    print('Total to process:', str(len(dois)))

    thread_counter = 0
    for doi in dois:
        while threading.active_count() >= THREAD_LIMIT:
            time.sleep(0.25)
        thread = AskPubMed(thread_counter, "thread-" + str(thread_counter), doi)
        thread_counter += 1
        thread.start()

if __name__ == '__main__':
    main()
