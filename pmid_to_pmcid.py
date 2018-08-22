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
        pmid.append(wikidata_to_pmid[wd_item])
    print('Done')

    print('Total to process:', str(len(pmid)))

    packages = [pmid[x:x+200] for x in range(0, len(pmid), 200)]

    for package in packages:
        new_package = []
        query_string = ""
        for item in package:
            query_string += item + ","
            if REDIS.get('pmid_to_pmcid__' + item) is not None:
                new_package.append(item)
        if len(new_package) == 0:
            continue

        query_string = query_string[:-1]  # Remove trailing comma
        s = requests.get(url + query_string)
        try:
            blob = s.json()
        except ValueError:
            continue

        to_lookup = []
        manifest = {}

        if "records" in blob:
            for response in blob["records"]:
                if "pmcid" in response:
                    response_pmcid = response["pmcid"].replace("PMC", "")
                    REDIS.set(
                        'pmid_to_pmcid__' + response["pmid"],
                        response_pmcid)

                    to_lookup.append(response["pmid"])
                    manifest[response["pmid"]] = {"P932": response_pmcid}
                    if "doi" in response:
                        manifest[response["pmid"]]["P356"] = response["doi"].upper()

                else:
                    REDIS.setex('pmid_to_pmcid__' + response["pmid"], '', timedelta(days=14))

        if len(to_lookup) > 0:
            pmid_to_wikidata = {}
            lookup = codeswitch.pmid_to_wikidata(to_lookup)
            for position, result in enumerate(lookup):
                if result is None:
                    continue
                pmid_to_wikidata[to_lookup[position]] = result

            for pmid, blob in manifest.items():
                for p_id, val in blob.items():
                    print(pmid_to_wikidata[pmid] + "\t" + p_id + "\t\"" + val + "\"")

if __name__ == '__main__':
    main()
