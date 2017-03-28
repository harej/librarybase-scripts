import requests

esummary_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&retmode=json&tool=wikidata_worker&email=jamesmhare@gmail.com&id="

issn_query_url = "https://query.wikidata.org/sparql?format=json&query=select%20%3Fi%20%3Fissn%20where%20%7B%20%3Fi%20wdt%3AP236%20%3Fissn%20%7D"
issn_seed = requests.get(issn_query_url).json()["results"]["bindings"]
issn_in_wikidata = [x["issn"]["value"] for x in issn_seed]

pubmed_seed = requests.get("https://query.wikidata.org/sparql?format=json&query=select%20%3Fpmid%20where%20%7B%20%0A%20%20%3Fitem%20wdt%3AP698%20%3Fpmid%20.%0A%20%20optional%20%7B%3Fitem%20wdt%3AP1433%20%3Fx%7D%0A%20%20filter%28%21bound%28%3Fx%29%29%0A%7D").json()
pmid_list = [x["pmid"]["value"] for x in pubmed_seed["results"]["bindings"]]
pmid_list_packages = [pmid_list[x:x+200] for x in range(0, len(pmid_list), 200)]

counter = 0
pubmed_issn = []
for package in pmid_list_packages:
    counter += 1
    print("Processing: " + str(counter) + "/" + str(len(pmid_list_packages)), end="\r")

    bunch_of_numbers = ""
    for pmid in package:
        bunch_of_numbers += pmid + ","
    bunch_of_numbers = bunch_of_numbers[:-1]  # Remove trailing comma

    summary_retriever = requests.get(esummary_url + bunch_of_numbers)

    if summary_retriever.status_code != 200:
        raise Exception(str(summary_retriever.text))

    summary_retriever_json = summary_retriever.json()
    if "result" in summary_retriever_json:
        for _, pmid_blob in summary_retriever_json["result"].items():
            if _ == "uids":
                continue

            if "issn" in pmid_blob:
                pubmed_issn.append(pmid_blob["issn"].upper())

print("\nThere are " + str(len(pubmed_issn)) + ".")

pubmed_issn = list(set(pubmed_issn) - set(issn_in_wikidata))

print("\nThere are " + str(len(pubmed_issn)) + " to be created.")

for issn in pubmed_issn:
    worldcat = requests.get("http://xissn.worldcat.org/webservices/xid/issn/{0}?format=json&method=getMetadata&fl=title".format(issn))
    if worldcat.status_code == 200:
        worldcat = worldcat.json()
        if "group" in worldcat:
            if len(worldcat["group"]) == 1:
                if "list" in worldcat["group"][0]:
                    if "issn" in worldcat["group"][0]["list"][0]:  # IS THIS ENOUGH ERROR HANDLING FOR YOU???
                        print("CREATE")
                        print("LAST\tP236\t\"" + issn + "\"")
                        if "title" in worldcat["group"][0]["list"][0]:
                            print("LAST\tLen\t\"" + worldcat["group"][0]["list"][0]["title"] + "\"")
                        print("LAST\tDen\t\"journal\"")
                        print("LAST\tP31\tQ5633421")