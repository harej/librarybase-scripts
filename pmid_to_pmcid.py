import redis
import requests
from datetime import timedelta

REDIS = redis.Redis(host='127.0.0.1', port=6379)

def main():
    seed = "https://query.wikidata.org/sparql?format=json&query=SELECT%20%3Fitem%20%3Fpmid%20WHERE%20%7B%0A%20%20%3Fitem%20wdt%3AP698%20%3Fpmid%20.%0A%20%20OPTIONAL%20%7B%20%3Fitem%20wdt%3AP932%20%3Fdummy1%20%7D%0A%20%20FILTER%28%21bound%28%3Fdummy1%29%29%0A%7D"
    url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?format=json&tool=wikidata_worker&email=jamesmhare@gmail.com&ids="

    r = requests.get(seed)
    blob = r.json()

    wikidata_items = {}  # key: pmid; value: wikidata item
    pmid = []
    for result in blob["results"]["bindings"]:
        wikidata_items[result["pmid"]["value"]] = result["item"]["value"].replace("http://www.wikidata.org/entity/", "")
        to_add = result["pmid"]["value"]
        if REDIS.get('pmid_to_pmcid__' + to_add) is None:
            pmid.append(to_add)

    packages = [pmid[x:x+200] for x in range(0, len(pmid), 200)]

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
                    found = True
                    print(wikidata_items[response["pmid"]] + "\tP932\t\"" + response["pmcid"].replace("PMC", "") + "\"")
                    REDIS.set(
                        'pmid_to_pmcid__' + response["pmid"],
                        response["pmcid"].replace("PMC", ""))
                    if "doi" in response:
                        print(wikidata_items[response["pmid"]] + "\tP356\t\"" + response["doi"].upper() + "\"")

                else:
                    REDIS.setex('pmid_to_pmcid__' + response["pmid"], '', timedelta(days=14))

if __name__ == '__main__':
    main()