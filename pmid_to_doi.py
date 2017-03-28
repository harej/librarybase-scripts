import requests
import csv
import threading
from bs4 import BeautifulSoup

class AskFriends(threading.Thread):
    def __init__(self, threadID, name, packages):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.packages = packages

    def run(self):
        new_csv = []
        counter = 1

        for package in self.packages:
            print(self.name + ": processing " + str(counter))
            counter += 1

            item = package["item"]
            pmid = package["pmid"]
            original_title = package["original_title"]

            t = requests.post("http://watcut.uwaterloo.ca/tools/pmid2doi", params={"pmid": pmid})
            soup = BeautifulSoup(t.text, "html.parser")
            crossref_url = soup.find_all("meta")
            if len(crossref_url) > 0:
                crossref_url = crossref_url[0].get("content")[7:]
            else:
                continue
            
            u = requests.get(crossref_url, headers={"Accept": "application/json"})
            try:
                crossref_blob = u.json()
            except ValueError:
                continue

            if "DOI" in crossref_blob:
                doi = crossref_blob["DOI"]
                if "title" in crossref_blob:
                    found_title = crossref_blob["title"]
                    new_csv.append({
                                     "original_title": original_title,
                                     "found_title": found_title,
                                     "item": item,
                                     "P356": "P356",
                                     "doi": doi
                                   })

        with open("pmid_to_doi-" + self.name + ".csv", "w") as f:
            writer = csv.DictWriter(f, fieldnames=["original_title", "found_title", "item", "P356", "doi"])
            writer.writeheader()
            for entry in new_csv:
                writer.writerow(entry)

def main():
    wdqs_query = "SELECT%20%3Fitem%20%3Flabel%20%3Fpmid%20WHERE%20%7B%0A%20%20%3Fitem%20wdt%3AP698%20%3Fpmid%20.%0A%20%20%3Fitem%20rdfs%3Alabel%20%3Flabel%20.%0A%20%20OPTIONAL%20%7B%20%3Fitem%20wdt%3AP356%20%3Fdummy1%20%7D%0A%20%20FILTER%28%21bound%28%3Fdummy1%29%29%0A%7D"
    seed_url = "https://query.wikidata.org/sparql?format=json&query=" + wdqs_query

    r = requests.get(seed_url)
    if r.status_code != 200:
        raise HTTPError

    query_blob = r.json()
    to_process = []
    for result in query_blob["results"]["bindings"]:
        item = result["item"]["value"].replace("http://www.wikidata.org/entity/", "")
        pmid = result["pmid"]["value"]
        original_title = result["label"]["value"]
        to_process.append({"item": item, "pmid": pmid, "original_title": original_title})

    package_of_packages = [to_process[x:x+1000] for x in range(0, len(to_process), 1000)]
    thread_counter = 0
    for packages in package_of_packages:
        thread = AskFriends(thread_counter, "thread-" + str(thread_counter), packages)
        thread_counter += 1
        thread.start()

main()