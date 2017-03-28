import requests
import threading

already_processed = []

class AskCrossref(threading.Thread):
    def __init__ (self, threadID, name, package, issn_in_wikidata):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.package = package
        self.issn_in_wikidata = issn_in_wikidata

    def run(self):
        global already_processed  # I'm going to hell
        for doi in self.package:
            try:
                r = requests.get("https://dx.doi.org/" + doi, headers={"Accept": "application/json"})
            except:
                continue
            if r.status_code != 200:
                continue

            try:
                blob = r.json()
            except:
                continue
            if "ISSN" in blob:
                for issn in blob["ISSN"]:
                    if issn in self.issn_in_wikidata:
                        continue
                    if issn in already_processed:
                        continue
                    worldcat = requests.get("http://xissn.worldcat.org/webservices/xid/issn/{0}?format=json&method=getMetadata&fl=title".format(issn))
                    if worldcat.status_code == 200:
                        try:
                            worldcat = worldcat.json()
                        except:
                            continue
                        if "group" in worldcat:
                            if len(worldcat["group"]) == 1:
                                if "list" in worldcat["group"][0]:
                                    if "issn" in worldcat["group"][0]["list"][0]:  # IS THIS ENOUGH ERROR HANDLING FOR YOU???
                                        output_string = ""
                                        output_string += "CREATE\n"
                                        output_string += "LAST\tP236\t\"" + issn + "\"\n"
                                        if "title" in worldcat["group"][0]["list"][0]:
                                            output_string += "LAST\tLen\t\"" + worldcat["group"][0]["list"][0]["title"] + "\"\n"
                                        output_string += "LAST\tDen\t\"journal\"\n"
                                        output_string += "LAST\tP31\tQ5633421"
                                        print(output_string)
                                        already_processed.append(issn)


def main():
    issn_query_url = "https://query.wikidata.org/sparql?format=json&query=select%20%3Fi%20%3Fissn%20where%20%7B%20%3Fi%20wdt%3AP236%20%3Fissn%20%7D"
    issn_seed = requests.get(issn_query_url).json()["results"]["bindings"]
    issn_in_wikidata = [x["issn"]["value"] for x in issn_seed]

    doi_seed = requests.get("https://query.wikidata.org/sparql?format=json&query=select%20%3Fdoi%20where%20%7B%20%0A%20%20%3Fitem%20wdt%3AP356%20%3Fdoi%20.%0A%20%20optional%20%7B%3Fitem%20wdt%3AP1433%20%3Fx%7D%0A%20%20filter%28%21bound%28%3Fx%29%29%0A%7D").json()
    doi_list = [x["doi"]["value"] for x in doi_seed["results"]["bindings"]]
    doi_packages = [doi_list[x:x+1000] for x in range(0, len(doi_list), 1000)]

    thread_counter = 0
    for package in doi_packages:
        thread = AskCrossref(thread_counter, "thread-" + str(thread_counter), package, issn_in_wikidata)
        thread_counter += 1
        thread.start()

if __name__ == "__main__":
    main()