import redis
import requests
import threading
from datetime import timedelta

REDIS = redis.Redis(host='127.0.0.1', port=6379)

class AskPubMed(threading.Thread):
    def __init__(self, threadID, name, package):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.package = package

    def run(self):
        for item in self.package:
            wikidata_item = item[0]
            doi = item[1]

            if REDIS.get('doi_to_pmid__' + doi) is not None:
                continue

            try:
                r = requests.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?retmode=json&db=pubmed&term=" + doi).json()
            except (OSError, ValueError):
                continue

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
    prefix = "http://www.wikidata.org/entity/"
    seed = "https://query.wikidata.org/sparql?format=json&query=SELECT%20%3Fi%20%3Fd%20WHERE%20%7B%0A%20%20%3Fi%20wdt%3AP356%20%3Fd%20.%0A%20%20OPTIONAL%20%7B%20%3Fi%20wdt%3AP698%20%3Fp%20%7D%0A%20%20FILTER%28%21bound%28%3Fp%29%29%0A%7D%0AORDER%20BY%20%3Fi"
    r = requests.get(seed).json()
    items = [(x["i"]["value"].replace(prefix, ""), x["d"]["value"]) for x in r["results"]["bindings"]]
    packages = [items[x:x+3000] for x in range(0, len(items), 3000)]
    thread_counter = 0
    for package in packages:
        thread = AskPubMed(thread_counter, "thread-" + str(thread_counter), package)
        thread_counter += 1
        thread.start()

if __name__ == '__main__':
    main()