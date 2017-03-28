import requests
import html

print("Generating from seed...")
seed_url = "https://query.wikidata.org/sparql?format=json&query=select%20%3Fi%20%3Fpmid%20where%20%7B%0A%20%20%20%20%3Fi%20wdt%3AP698%20%3Fpmid%20.%0A%20%20%20%20optional%20%7B%20%3Fi%20wdt%3AP1476%20%3Ftitle%20.%20%7D%0A%20%20%20%20filter%28%21bound%28%3Ftitle%29%29%0A%7D%0Aorder%20by%20%3Fi"
seed = requests.get(seed_url).json()["results"]["bindings"]
pmid_to_wikidata = {x["i"]["value"].replace("http://www.wikidata.org/entity/", ""): x["pmid"]["value"] for x in seed}

print("Now retrieving from PubMed...")
for wikidata_item, pmid in pmid_to_wikidata.items():
    esummary_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&retmode=json&tool=wikidata_worker&email=jamesmhare@gmail.com&id=" + pmid
    retriever = requests.get(esummary_url)
    try:
        retriever_blob = retriever.json()
    except:
        print(retriever.text)

    if "result" not in retriever_blob:
        continue
    if pmid not in retriever_blob["result"]:
        continue

    result = retriever_blob["result"][pmid]

    if "title" in result:
        t = html.unescape(result["title"])
        if t != "":
            if t[-1:] == ".":
                t = t[:-1]
            if t[0] == "[":
                t = t[1:]
            if t[-1:] == "]":
                t = t[:-1]
            print(wikidata_item + "\tLen\t\"" + t + "\"")
            print(wikidata_item + "\tP1476\ten:\"" + t + "\"")