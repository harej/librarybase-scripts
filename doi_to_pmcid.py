import requests
import codeswitch

def main():
    url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?format=json&tool=wikidata_worker&email=jamesmhare@gmail.com&ids="

    print('Getting all Wikidata items with P356...')
    wikidata_to_doi = codeswitch.hgetall('wikidata_to_P356')
    print('Done')

    print('Getting all Wikidata items with P932 (so we can filter them out)...')
    wikidata_to_pmcid = codeswitch.hgetall('wikidata_to_P932')
    print('Done')

    whitelist = list(wikidata_to_doi.keys())
    blacklist = list(wikidata_to_pmcid.keys())

    print('Filtering out the ones we don\'t need to process...')
    whitelist = list(set(whitelist) - set(blacklist))
    doi = []
    for wd_item in whitelist:
        doi.append(wikidata_to_doi[wd_item])
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
                    get_wd = codeswitch.doi_to_wikidata(responsedoi)
                    if get_wd is not None:
                        print(get_wd + "\tP932\t\"" + response["pmcid"].replace("PMC", "") + "\"")
                        if "pmid" in response:
                            print(get_wd + "\tP698\t\"" + response["pmid"] + "\"")

if __name__ == '__main__':
    main()
