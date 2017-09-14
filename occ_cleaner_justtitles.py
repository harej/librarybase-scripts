import editdistance
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
                                wikidata_title = block['mainsnak']['datavalue']['value']['text']
                                print(wikidata_title + '\tWikidata')
        if title_found is False:
            wikidata_title = item_engine.get_label()
            print(wikidata_title + '\tWikidata')

        autodelete = []

        if 'P932' in claims:  # pmcid
            for index, block in enumerate(claims['P932']):
                res_id = 'c' + str(index)
                if 'mainsnak' in block:
                    if 'datavalue' in block['mainsnak']:
                        if 'value' in block['mainsnak']['datavalue']:
                            pmcid = block['mainsnak']['datavalue']['value']
                            blob = JournalArticles.get_pubmed_central([pmcid])
                            for identifier, result in blob.items():
                                if 'title' in result:
                                    print(result['title'] + '\t' + res_id)
                                    if editdistance.eval(wikidata_title.upper(), result['title'].upper()) > 10:
                                        autodelete.append(res_id)

        if 'P698' in claims:  # pmid
            for index, block in enumerate(claims['P698']):
                res_id = 'p' + str(index)
                if 'mainsnak' in block:
                    if 'datavalue' in block['mainsnak']:
                        if 'value' in block['mainsnak']['datavalue']:
                            pmid = block['mainsnak']['datavalue']['value']
                            blob = JournalArticles.get_pubmed([pmid])
                            for identifier, result in blob.items():
                                if 'title' in result:
                                    print(result['title'] + '\t' + res_id)
                                    if editdistance.eval(wikidata_title.upper(), result['title'].upper()) > 10:
                                        autodelete.append(res_id)

        if 'P356' in claims:  # doi
            for index, block in enumerate(claims['P356']):
                res_id = 'd' + str(index)
                if 'mainsnak' in block:
                    if 'datavalue' in block['mainsnak']:
                        if 'value' in block['mainsnak']['datavalue']:
                            doi = block['mainsnak']['datavalue']['value']
                            try:
                                blob = requests.get('https://api.crossref.org/works/' + doi).json()['message']
                                if 'title' in blob:
                                    print(blob['title'][0] + '\t' + res_id)
                                    if editdistance.eval(wikidata_title.upper(), blob['title'][0].upper()) > 10:
                                        autodelete.append(res_id)
                            except Exception as e:
                                print('ERROR: ' + str(e) + '\t' + res_id)

        if 'P3181' in claims:  # opencitations corpus
            for index, block in enumerate(claims['P3181']):
                res_id = 'o' + str(index)
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
                            if 'http://purl.org/dc/terms/title' in blob:
                                print(blob['http://purl.org/dc/terms/title'][0]['@value'] + '\t' + res_id)
                                if editdistance.eval(wikidata_title.upper(), blob['http://purl.org/dc/terms/title'][0]['@value'].upper()) > 10:
                                    autodelete.append(res_id)

        autodelete_string = ''
        for x in autodelete:
            autodelete_string += x + ' '
        autodelete_string = autodelete_string[:-1]

        print('Press a to autodelete: ' + autodelete_string)
        raw_response = input('> ')

        if raw_response in ['', 'none']:
            continue

        if raw_response == 'a':
            raw_response = autodelete_string

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
