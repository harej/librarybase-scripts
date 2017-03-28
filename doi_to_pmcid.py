import requests

def main():
    seed = "https://query.wikidata.org/sparql?format=json&query=SELECT%20%3Fitem%20%3Fdoi%20WHERE%20%7B%0A%20%20%3Fitem%20wdt%3AP356%20%3Fdoi%20.%0A%20%20OPTIONAL%20%7B%20%3Fitem%20wdt%3AP932%20%3Fdummy1%20%7D%0A%20%20FILTER%28%21bound%28%3Fdummy1%29%29%0A%7D"
    url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?format=json&tool=wikidata_worker&email=jamesmhare@gmail.com&ids="

    r = requests.get(seed)
    blob = r.json()

    wikidata_items = {}  # key: doi; value: wikidata item
    doi = []
    for result in blob["results"]["bindings"]:
        wikidata_items[result["doi"]["value"]] = result["item"]["value"].replace("http://www.wikidata.org/entity/", "")
        doi.append(result["doi"]["value"])

    packages = [doi[x:x+200] for x in range(0, len(doi), 200)]

    for package in packages:
        query_string = ""
        for item in package:
            query_string += item + ","
        query_string = query_string[:-1]  # Remove trailing comma

        s = requests.get(url + query_string)
        try:
            blob = s.json()
        except ValueError:
            continue

        if "records" in blob:
            for response in blob["records"]:
                if "pmcid" in response:
                    print(wikidata_items[response["doi"]] + "\tP932\t\"" + response["pmcid"].replace("PMC", "") + "\"")
                    if "pmid" in response:
                        print(wikidata_items[response["doi"]] + "\tP698\t\"" + response["pmid"] + "\"")

if __name__ == '__main__':
    main()