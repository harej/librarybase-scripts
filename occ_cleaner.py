import requests
from BiblioWikidata import JournalArticles
from wikidataintegrator import wdi_core, wdi_login
from site_credentials import *

WIKI_SESSION = wdi_login.WDLogin(user=site_username, pwd=site_password)

def do(prop):
    q = 'https://query.wikidata.org/sparql?format=json&query=SELECT%20DISTINCT%20%3Fitem%0A%7B%7BSELECT%20%3Fitem%20%28COUNT%28%3Fvalue%29%20AS%20%3Fcount%29%20%28GROUP_CONCAT%28%3Fvalue%3B%20separator%3D%22%2C%20%22%29%20AS%20%3FvalueList%29%20%7B%0A%3Fitem%20wdt%3A{0}%20%3Fvalue%20.%0A%7D%20GROUP%20BY%20%3Fitem%20%7D%20.%0A%3Fitem%20wdt%3AP3181%20%3Fbr%20.%0AFILTER%28%3Fcount%20%3E%201%29%20.%0A%7D%20ORDER%20BY%20DESC%28%3Fcount%29'
    manifest = [x['item']['value'].replace('http://www.wikidata.org/entity/', '') for x in requests.get(q.format(prop)).json()['results']['bindings']]

    for item in manifest:
        item_engine = wdi_core.WDItemEngine(wd_item_id=item)
        print('#' * 80)
        print('Item: ' + item)
        claims = item_engine.wd_json_representation['claims']

        title_found = False
        if 'P1476' in claims:
            for block in claims['P1476']:
                if 'mainsnak' in block:
                    if 'datavalue' in block['mainsnak']:
                        if 'value' in block['mainsnak']['datavalue']:
                            if 'text' in block['mainsnak']['datavalue']['value']:
                                title_found = True
                                print('Title: ' + block['mainsnak']['datavalue']['value']['text'])
        if title_found is False:
            print('Title: ' + item_engine.get_label())

        pub_date_found = False
        if 'P577' in claims:
            for block in claims['P577']:
                if 'mainsnak' in block:
                    if 'datavalue' in block['mainsnak']:
                        if 'value' in block['mainsnak']['datavalue']:
                            if 'time' in block['mainsnak']['datavalue']['value']:
                                pub_date_found = True
                                print('Date: ' + block['mainsnak']['datavalue']['value']['time'][:11])
        if pub_date_found is False:
            print('Date: UNKNOWN')

        published_in_found = False
        if 'P1433' in claims:
            for block in claims['P1433']:
                if 'mainsnak' in block:
                    if 'datavalue' in block['mainsnak']:
                        if 'value' in block['mainsnak']['datavalue']:
                            if 'id' in block['mainsnak']['datavalue']['value']:
                                published_in_found = True
                                journal_item_engine = wdi_core.WDItemEngine(block['mainsnak']['datavalue']['value']['id'])
                                print('Published in: ' + journal_item_engine.get_label())
        if published_in_found is False:
            print('Published in: UNKNOWN')

        print('\n')

        if 'P932' in claims:  # pmcid
            for index, block in enumerate(claims['P932']):
                res_id = 'c' + str(index) + ': '
                if 'mainsnak' in block:
                    if 'datavalue' in block['mainsnak']:
                        if 'value' in block['mainsnak']['datavalue']:
                            pmcid = block['mainsnak']['datavalue']['value']
                            blob = JournalArticles.get_pubmed_central([pmcid])
                            for identifier, result in blob.items():
                                print(res_id + 'PMCID: ' + identifier)
                                if 'title' in result:
                                    print(res_id + 'Title: ' + result['title'])
                                if 'pubdate' in result:
                                    print(res_id + 'Date: ' + result['pubdate'])
                                if 'fulljournalname' in result:
                                    print(res_id + 'Published in: ' + result['fulljournalname'])
                            print('\n')

        if 'P698' in claims:  # pmid
            for index, block in enumerate(claims['P698']):
                res_id = 'p' + str(index) + ': '
                if 'mainsnak' in block:
                    if 'datavalue' in block['mainsnak']:
                        if 'value' in block['mainsnak']['datavalue']:
                            pmid = block['mainsnak']['datavalue']['value']
                            blob = JournalArticles.get_pubmed([pmid])
                            for identifier, result in blob.items():
                                print(res_id + 'PMID: ' + identifier)
                                if 'title' in result:
                                    print(res_id + 'Title: ' + result['title'])
                                if 'pubdate' in result:
                                    print(res_id + 'Date: ' + result['pubdate'])
                                if 'fulljournalname' in result:
                                    print(res_id + 'Published in: ' + result['fulljournalname'])
                            print('\n')

        if 'P356' in claims:  # doi
            for index, block in enumerate(claims['P356']):
                res_id = 'd' + str(index) + ': '
                if 'mainsnak' in block:
                    if 'datavalue' in block['mainsnak']:
                        if 'value' in block['mainsnak']['datavalue']:
                            doi = block['mainsnak']['datavalue']['value']
                            try:
                                blob = requests.get('https://api.crossref.org/works/' + doi).json()['message']
                                print(res_id + 'DOI: ' + doi)
                                if 'title' in blob:
                                    print(res_id + 'Title: ' + blob['title'][0])
                                if 'published-online' in blob:
                                    if 'date-parts' in blob['published-online']:
                                        date = ''
                                        for x in blob['published-online']['date-parts'][0]:
                                            date += str(x) + ' '
                                        print(res_id + 'Date: ' + date)
                                if 'container-title' in blob:
                                    print(res_id + 'Published in: ' + blob['container-title'][0])
                            except Exception as e:
                                print(res_id + 'ERROR: ' + str(e))
                            finally:
                                print('\n')

        if 'P3181' in claims:  # opencitations corpus
            for index, block in enumerate(claims['P3181']):
                res_id = 'o' + str(index) + ': '
                if 'mainsnak' in block:
                    if 'datavalue' in block['mainsnak']:
                        if 'value' in block['mainsnak']['datavalue']:
                            br = block['mainsnak']['datavalue']['value']
                            blob = requests.get('http://opencitations.net/corpus/br/{0}.json'.format(br)).json()
                            if len(blob) == 1:
                                blob = blob[0]
                            else:
                                for subblob in blob:
                                    if subblob['@id'] != '':
                                        blob = subblob
                                        break
                            print(res_id + 'OCC BR: ' + br)
                            if 'http://purl.org/dc/terms/title' in blob:
                                print(res_id + 'Title: ' + blob['http://purl.org/dc/terms/title'][0]['@value'])
                            if 'http://purl.org/spar/fabio/hasPublicationYear' in blob:
                                print(res_id + 'Date: ' + blob['http://purl.org/spar/fabio/hasPublicationYear'][0]['@value'])
                            print('\n')

        print('Which IDs should be deleted?')
        raw_response = input('> ')

        if raw_response in ['', 'none']:
            continue

        responses = raw_response.split(' ')

        mapping = {'c': 'P932', 'p': 'P698', 'd': 'P356', 'o': 'P3181'}
        for response in responses:
            code = response[0]
            index = int(response[1:])
            related_prop = mapping[code]
            offending_value = claims[related_prop][index]['mainsnak']['datavalue']['value']

            with open('occ_cleaner.txt', 'a') as f:
                f.write('-' + item + '|' + related_prop + '|"' + offending_value + '"\n')

def main():
    for prop in ['P932', 'P698', 'P356', 'P3181']:
        do(prop)

if __name__ == '__main__':
    main()
