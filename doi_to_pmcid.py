import requests
import codeswitch
from pprint import pprint
import random

def main():
    url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?format=json&tool=wikidata_worker&email=jamesmhare@gmail.com&ids="

    print('Getting all Wikidata items with P356...')
    doi_to_wikidata = codeswitch.hgetall('P356_to_wikidata')
    doi_to_wikidata = {x.upper(): y for x, y in doi_to_wikidata.items()}
    print('Done')

    print('Getting all Wikidata items with P932 (so we can filter them out)...')
    wikidata_to_pmcid = codeswitch.hgetall('wikidata_to_P932')
    print('Done')

    blacklist = list(wikidata_to_pmcid.keys())

    pprint(random.sample(doi_to_wikidata.items(), 5))
    pprint(random.sample(blacklist, 5))

    doi = []
    print('Filtering out the ones we don\'t need to process...')
    for identifier, wd_item in doi_to_wikidata.items():
        if wd_item not in blacklist:
            doi.append(identifier)
    print('Done')

    packages = [doi[x:x+200] for x in range(0, len(doi), 200)]

    for package in packages:
        query_string = ""
        for item in package:
            query_string += item + ","
        query_string = query_string[:-1]  # Remove trailing comma

        s = requests.get(url + query_string)
        try:
            blob = s.json()
        except ValueError as e:
            print("Error!", str(e))

        if "records" in blob:
            for response in blob["records"]:
                responsedoi = response["doi"].upper()
                if "pmcid" in response:
                    print(doi_to_wikidata[responsedoi] + "\tP932\t\"" + response["pmcid"].replace("PMC", "") + "\"")
                    if "pmid" in response:
                        print(doi_to_wikidata[responsedoi] + "\tP698\t\"" + response["pmid"] + "\"")

if __name__ == '__main__':
    main()