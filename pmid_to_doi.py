import codeswitch
import re
import requests
from BiblioWikidata import JournalArticles
from datetime import timedelta

def main():
    print('Getting all Wikidata items with P698...')
    wikidata_to_pmid = codeswitch.hgetall('wikidata_to_P698')
    print('Done')

    print('Getting all Wikidata items with P356 (so we can filter them out)...')
    wikidata_to_doi = codeswitch.hgetall('wikidata_to_P356')
    print('Done')

    whitelist = list(wikidata_to_pmid.keys())
    blacklist = list(wikidata_to_doi.keys())

    print('Filtering out the ones we don\'t need to process...')
    whitelist = list(set(whitelist) - set(blacklist))
    pmid_list = []
    for wd_item in whitelist:
        pmid_list.append(wikidata_to_pmid[wd_item])
    print('Done')

    packages = [pmid_list[x:x+200] for x in range(0, len(pmid_list), 200)]

    for package in packages:
        data = JournalArticles.get_pubmed(package)

        for pmid, entry in data.items():
            doi = None
            if 'articleids' in entry:
                for block in entry['articleids']:
                    if block['idtype'] == 'doi':
                        doi = block['value'].upper()
                        break

            if doi is not None:
                if re.match(r'^10\.\d+\/.+$', doi) is not None:
                    get_wd = codeswitch.pmid_to_wikidata(pmid)
                    if get_wd is not None:
                        print(get_wd + '\tP356\t"' + doi + '"')

if __name__ == '__main__':
    main()
