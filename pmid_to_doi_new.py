import redis
import requests
from BiblioWikidata import JournalArticles
from datetime import timedelta

REDIS = redis.Redis(host='127.0.0.1', port=6379)

def main():
    seed = 'https://query.wikidata.org/sparql?format=json&query=SELECT%20%3Fitem%20%3Fpmid%20WHERE%20%7B%0A%20%20%3Fitem%20wdt%3AP698%20%3Fpmid%20.%0A%20%20OPTIONAL%20%7B%20%3Fitem%20wdt%3AP356%20%3Fx%20%7D%0A%20%20FILTER%28%21bound%28%3Fx%29%29%0A%7D'

    r = requests.get(seed).json()['results']['bindings']

    pmid_to_wikidata = {}
    pmid_list = []
    for entry in r:
        wd = entry['item']['value'].replace('http://www.wikidata.org/entity/', '')
        pmid = str(entry['pmid']['value'])

        if REDIS.get('pmid_to_doi_new__' + pmid) is None:
            pmid_to_wikidata[pmid] = wd
            pmid_list.append(pmid)

    data = JournalArticles.get_pubmed(pmid_list)

    for pmid, entry in data.items():
        doi = None
        if 'articleids' in entry:
            for block in entry['articleids']:
                if block['idtype'] == 'doi':
                    doi = block['value'].upper()
                    break

        if doi is not None:
            print(pmid_to_wikidata[pmid] + '\tP356\t"' + doi + '"')
            REDIS.set('pmid_to_doi_new__' + pmid, doi)
        else:
            REDIS.set('pmid_to_doi_new__' + pmid, '', timedelta(days=14))

if __name__ == '__main__':
    main()
