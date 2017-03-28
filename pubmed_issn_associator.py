import requests

esummary_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&retmode=json&tool=wikidata_worker&email=jamesmhare@gmail.com&id="

issn_query_url = "https://query.wikidata.org/sparql?format=json&query=select%20%3Fi%20%3Fissn%20where%20%7B%20%3Fi%20wdt%3AP236%20%3Fissn%20%7D"
issn_seed = requests.get(issn_query_url).json()["results"]["bindings"]
issn_to_wikidata = {x["issn"]["value"]: x["i"]["value"].replace("http://www.wikidata.org/entity/", "") for x in issn_seed}

pubmed_seed = requests.get("https://query.wikidata.org/sparql?format=json&query=select%20%3Fitem%20%3Fpmid%20where%20%7B%20%0A%20%20%3Fitem%20wdt%3AP698%20%3Fpmid%20.%0A%20%20optional%20%7B%3Fitem%20wdt%3AP1433%20%3Fx%7D%0A%20%20filter%28%21bound%28%3Fx%29%29%0A%7D").json()
pmid_to_wikidata = {x["pmid"]["value"]: x["item"]["value"].replace("http://www.wikidata.org/entity/", "") for x in pubmed_seed["results"]["bindings"]}
pmid_list = list(pmid_to_wikidata.keys())
pmid_list_packages = [pmid_list[x:x+200] for x in range(0, len(pmid_list), 200)]

for package in pmid_list_packages:
    bunch_of_numbers = ""
    for pmid in package:
        bunch_of_numbers += pmid + ","
    bunch_of_numbers = bunch_of_numbers[:-1]  # Remove trailing comma

    summary_retriever = requests.get(esummary_url + bunch_of_numbers)

    if summary_retriever.status_code != 200:
        raise Exception(str(summary_retriever.text))

    summary_retriever_json = summary_retriever.json()
    if "result" in summary_retriever_json:
        for pmid, pmid_blob in summary_retriever_json["result"].items():
            if pmid == "uids":
                continue

            if "issn" in pmid_blob:
                issn = pmid_blob["issn"].upper()
                if issn in issn_to_wikidata:
                    article_item = pmid_to_wikidata[pmid]
                    issn_item = issn_to_wikidata[issn]
                    print(article_item + "\tP1433\t" + issn_item + "\tS248\tQ180686")