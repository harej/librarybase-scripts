import requests
import threading

class AskCrossref(threading.Thread):
    def __init__ (self, threadID, name, package, issn_to_wikidata, doi_to_wikidata):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.package = package
        self.issn_to_wikidata = issn_to_wikidata
        self.doi_to_wikidata = doi_to_wikidata

    def run(self):
        for doi in self.package:
            try:
                r = requests.get("https://doi.org/" + doi, headers={"Accept": "application/json"})
            except:  # fuck you
                continue
            if r.status_code != 200:
                continue
            try:
                blob = r.json()
            except:
                continue
            if "ISSN" in blob:
                issn_item_list = []
                for issn in blob["ISSN"]:
                    if issn in self.issn_to_wikidata:
                        issn_item_list.append(self.issn_to_wikidata[issn])
                issn_item_list = list(set(issn_item_list))
                for issn_item in issn_item_list:
                    article_item = self.doi_to_wikidata[doi]
                    if issn_item != article_item:
                        print(article_item + "\tP1433\t" + issn_item + "\tS248\tQ5188229")

def main():
    issn_query_url = "https://query.wikidata.org/sparql?format=json&query=select%20%3Fi%20%3Fissn%20where%20%7B%20%3Fi%20wdt%3AP236%20%3Fissn%20%7D"
    issn_seed = requests.get(issn_query_url).json()["results"]["bindings"]
    issn_to_wikidata = {x["issn"]["value"]: x["i"]["value"].replace("http://www.wikidata.org/entity/", "") for x in issn_seed}

    doi_seed = requests.get("https://query.wikidata.org/sparql?format=json&query=select%20%3Fitem%20%3Fdoi%20where%20%7B%20%0A%20%20%3Fitem%20wdt%3AP356%20%3Fdoi%20.%0A%20%20optional%20%7B%3Fitem%20wdt%3AP1433%20%3Fx%7D%0A%20%20filter%28%21bound%28%3Fx%29%29%0A%7D%0Aorder%20by%20%3Fitem").json()
    doi_to_wikidata = {x["doi"]["value"]: x["item"]["value"].replace("http://www.wikidata.org/entity/", "") for x in doi_seed["results"]["bindings"]}
    doi_list = list(doi_to_wikidata.keys())
    doi_packages = [doi_list[x:x+1000] for x in range(0, len(doi_list), 1000)]

    thread_counter = 0
    for package in doi_packages:
        thread = AskCrossref(thread_counter, "thread-" + str(thread_counter), package, issn_to_wikidata, doi_to_wikidata)
        thread_counter += 1
        thread.start()


if __name__ == '__main__':
    main()