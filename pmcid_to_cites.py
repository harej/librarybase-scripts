import arrow
import requests
from wikidataintegrator import wdi_core, wdi_login
from site_credentials import *

def main():
    WIKIDATA = wdi_login.WDLogin(user=site_username, pwd=site_password)
    pmcid_seed = "https://query.wikidata.org/sparql?format=json&query=select%20%3Fitem%20%3Fpmcid%20where%20%7B%20%3Fitem%20wdt%3AP932%20%3Fpmcid%20%7D"
    pmid_seed = "https://query.wikidata.org/sparql?format=json&query=SELECT%20%3Fitem%20%3Fpmid%20WHERE%20%7B%0A%20%20%3Fitem%20wdt%3AP698%20%3Fpmid%20.%0A%7D"
    pmc_template = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pmc&linkname=pmc_refs_pubmed&tool=wikidata_worker&email=jamesmhare@gmail.com&retmode=json"

    pmcid_list_raw = requests.get(pmcid_seed).json()["results"]["bindings"]
    pmcid_to_wikidata = {}
    pmcid_list = []  # list of tuples: (wikidata item, PMCID)
    for x in pmcid_list_raw:
        item = x["item"]["value"].replace("http://www.wikidata.org/entity/", "")
        pmcid = x["pmcid"]["value"]

        pmcid_to_wikidata[pmcid] = item
        pmcid_list.append((item, pmcid))

    get_pmid_list = requests.get(pmid_seed)
    pmid_blob = get_pmid_list.json()
    pmid_to_wikidata = {x["pmid"]["value"]: x["item"]["value"].replace("http://www.wikidata.org/entity/", "") for x in pmid_blob["results"]["bindings"]}

    pmcid_list = list(set(pmcid_list))  # Removing duplicates
    pmcid_list.sort(reverse=True)
    packages = [pmcid_list[x:x+200] for x in range(0, len(pmcid_list), 200)]

    print(str(len(packages)) + " packages")

    for package in packages:
        query_string = ""
        for item in package:
            query_string += "&id=" + item[1]

        r = requests.get(pmc_template + query_string)
        retrieve_date = arrow.utcnow().format('YYYY-MM-DD')
        retrieve_date = '+' + retrieve_date + 'T00:00:00Z'
        blob = r.json()

        for result in blob["linksets"]:
            if str(result["ids"][0]) not in pmcid_to_wikidata \
            or 'linksetdbs' not in result:
                continue
            relevant_pmcid = str(result["ids"][0])
            relevant_item = pmcid_to_wikidata[relevant_pmcid]

            i = wdi_core.WDItemEngine(wd_item_id=relevant_item)

            cites = []

            # Creating a convenient data object for keeping track of existing
            # 'cites' claims and their references on a given Wikidata item
            references = {}
            if 'P2860' in i.wd_json_representation['claims']:
                extant_claims = i.wd_json_representation['claims']['P2860']
                for claim in extant_claims:
                    extant_cited_item = claim['mainsnak']['datavalue']['value']['id']
                    
                    for reference in claim['references']:
                        snaks = {}
                        for prop_nr, values in reference['snaks'].items():
                            snaks[prop_nr] = [v['datavalue'] for v in values]

                    references[extant_cited_item] = snaks

            for cited_id in result["linksetdbs"][0]["links"]:
                if str(cited_id) not in pmid_to_wikidata:
                    continue
                cited_item = pmid_to_wikidata[str(cited_id)]
                if relevant_item == cited_item:
                    continue

                # Don't generate a statement if the statement already exists and
                # features a fully filled out citation. *Do* otherwise generate
                # a statement, even if the statement exists, if it has a lousy,
                # not-filled-out citation.
                generate_statement = True
                if cited_item in references:
                    if 'P248' in references[cited_item] \
                    and 'P813' in references[cited_item] \
                    and 'P854' in references[cited_item]:
                        for x in references[cited_item]['P248']:
                            # stated in: PubMed Central
                            if x['value']['id'] == 'Q229883':
                                generate_statement = False

                if generate_statement is False:
                    continue

                refblock  = [[wdi_core.WDItemID(
                                  value='Q229883',
                                  prop_nr='P248',
                                  is_reference=True),
                              wdi_core.WDUrl(
                                  value='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pmc&linkname=pmc_refs_pubmed&retmode=json&id=' + relevant_pmcid,
                                  prop_nr='P854',
                                  is_reference=True),
                              wdi_core.WDTime(
                                  retrieve_date,
                                  prop_nr='P813',
                                  is_reference=True)]]

                statement = wdi_core.WDItemID(
                                value=cited_item,
                                prop_nr='P2860',
                                references=refblock)

                cites.append(statement)

            if len(cites) > 0:
                i = wdi_core.WDItemEngine(
                        wd_item_id=relevant_item,
                        data=cites,
                        append_value=['P2860'],
                        good_refs=[{'P248': None, 'P813': None, 'P854': None}],
                        keep_good_ref_statements=True)
                try:
                    print(i.write(WIKIDATA))
                except Exception as e:
                    print('Exception when trying to edit ' + relevant_item + '; skipping')
                    print(e)
            else:
                print(relevant_item + ' has nothing to add; skipping')

if __name__ == '__main__':
    main()
