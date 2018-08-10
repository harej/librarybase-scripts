import requests
import codeswitch

def main():
    url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?format=json&tool=wikidata_worker&email=jamesmhare@gmail.com&ids="

    doi = []
    for identifier in codeswitch.identifier_generator('P356', 'doi_to_pmcid_list'):
        if codeswitch.wikidata_to_pmcid(codeswitch.doi_to_wikidata(identifier)) is None:
            doi.append(identifier)

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
                responsedoi = response["doi"].upper()
                if "pmcid" in response:
                    print(codeswitch.doi_to_wikidata(responsedoi) + "\tP932\t\"" + response["pmcid"].replace("PMC", "") + "\"")
                    if "pmid" in response:
                        print(codeswitch.doi_to_wikidata(responsedoi) + "\tP698\t\"" + response["pmid"] + "\"")

if __name__ == '__main__':
    main()