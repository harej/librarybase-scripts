import codeswitch
import redis
import requests
from datetime import timedelta
from site_credentials import *

REDIS = redis.Redis(host=redis_server, port=redis_port, password=redis_key)

def main():
    url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?format=json&tool=wikidata_worker&email=jamesmhare@gmail.com&ids="

    print('Getting all Wikidata items with P698...')
    wikidata_to_pmid = codeswitch.hgetall('wikidata_to_P698')
    print('Done')

    print('Getting all Wikidata items with P932 (so we can filter them out)...')
    wikidata_to_pmcid = codeswitch.hgetall('wikidata_to_P932')
    print('Done')

    whitelist = list(wikidata_to_pmid.keys())
    blacklist = list(wikidata_to_pmcid.keys())

    print('Filtering out the ones we don\'t need to process...')
    whitelist = list(set(whitelist) - set(blacklist))
    pmid = []
    for wd_item in whitelist:
        if REDIS.get('pmid_to_pmcid__' + wikidata_to_pmid[wd_item]) is None:
            pmid.append(wikidata_to_pmid[wd_item])
    print('Done')

    print('Total to process:', str(len(pmid)))

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
                    print(codeswitch.pmid_to_wikidata(response["pmid"]) + "\tP932\t\"" + response["pmcid"].replace("PMC", "") + "\"")
                    REDIS.set(
                        'pmid_to_pmcid__' + response["pmid"],
                        response["pmcid"].replace("PMC", ""))
                    if "doi" in response:
                        print(codeswitch.pmid_to_wikidata(response["pmid"]) + "\tP356\t\"" + response["doi"].upper() + "\"")

                else:
                    REDIS.setex('pmid_to_pmcid__' + response["pmid"], '', timedelta(days=14))

if __name__ == '__main__':
    main()
