import redis
import requests
import editdistance
import threading
import time
from datetime import timedelta

REDIS = redis.Redis(host='127.0.0.1', port=6379)

class AskCrossref(threading.Thread):
    def __init__ (self, threadID, name, entry):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.entry = entry

    def run(self):
        crossref = "https://api.crossref.org/works?query.title={0}"
        wikidata_item = self.entry["item"]["value"].replace("http://www.wikidata.org/entity/", "")
        title = self.entry["title"]["value"]
        volume = self.entry["vol"]["value"]
        issue = self.entry["issue"]["value"]

        try:
            crossref_data = requests.get(crossref.format(title)).json()
        except:
            return

        found = False
        if "message" in crossref_data:
            if "items" in crossref_data["message"]:
                crossref_data = crossref_data["message"]["items"]

                for result in crossref_data:
                    if "volume" in result and "issue" in result and "title" in result:
                        if result["volume"] == volume:
                            if result["issue"] == issue:
                                crossref_title = result["title"][0].upper()
                                if editdistance.eval(title.upper(), crossref_title) < 10:  # levenshtein distance
                                    found = True
                                    doi = result["DOI"].upper()
                                    print(wikidata_item + "\tP356\t\"" + doi + "\"")
                                    REDIS.set('pmid_to_doi__' + wikidata_item, doi)
                                    break
        if found is False:
            REDIS.setex('pmid_to_doi__' + wikidata_item, '', timedelta(days=14))

def main():
    seed = "https://query.wikidata.org/sparql?format=json&query=SELECT%20%3Fitem%20%3Ftitle%20%3Fvol%20%3Fissue%20WHERE%20%7B%0A%20%20%3Fitem%20wdt%3AP1476%20%3Ftitle%20.%0A%20%20%3Fitem%20wdt%3AP478%20%3Fvol%20.%0A%20%20%3Fitem%20wdt%3AP433%20%3Fissue%20.%0A%20%20OPTIONAL%20%7B%20%3Fitem%20wdt%3AP356%20%3Fdummy1%20%7D%0A%20%20FILTER%28%21bound%28%3Fdummy1%29%29%0A%7D%0Aorder%20by%20desc%28%3Fitem%29"
    seed = requests.get(seed).json()["results"]["bindings"]
    seed = [x for x in seed if REDIS.get('pmid_to_doi__' + x["item"]["value"].replace("http://www.wikidata.org/entity/", "")) is None]

    thread_counter = 0
    for entry in seed:
        thread = AskCrossref(thread_counter, "thread-" + str(thread_counter), entry)
        thread_counter += 1
        thread.start()
        time.sleep(.5)

if __name__ == '__main__':
    main()
