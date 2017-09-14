from BiblioWikidata import JournalArticles
import csv
import requests
from pprint import pprint
from urllib.parse import unquote

def main(manifestfile):

	do_not_generate = []
	to_generate = []

	seed = 'https://query.wikidata.org/sparql?format=json&query=select%20%3Fn%20where%20%7B%3Fi%20wdt%3AP356%20%3Fn%7D'
	r = requests.get(seed).json()['results']['bindings']

	for result in r:
		do_not_generate.append(result['n']['value'].upper())

	with open(manifestfile) as f:
		s = csv.reader(f)
		for row in s:
			to_add = row[0].upper()
			to_add = to_add.strip()
			to_add = unquote(to_add)
			to_add = to_add.replace('/abstract', '').replace('/full', '').replace('/pdf', '')
			to_generate.append(to_add)

	to_generate = list(set(to_generate) - set(do_not_generate))
	print(str(len(to_generate)) + ' items to generate.')

	for x in to_generate:
		manifest = [{'pmcid': None, 'doi': x, 'pmid': None}]
		JournalArticles.item_creator(manifest)

if __name__ == '__main__':
	main('wikipedia_doi.csv')
