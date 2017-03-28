from BiblioWikidata import JournalArticles
import csv
import requests
from pprint import pprint

def main(manifestfile):

	do_not_generate = []
	to_generate = []

	seed = 'https://query.wikidata.org/sparql?format=json&query=select%20%3Fn%20where%20%7B%3Fi%20wdt%3AP932%20%3Fn%7D'
	r = requests.get(seed).json()['results']['bindings']

	for result in r:
		do_not_generate.append(result['n']['value'])

	with open(manifestfile) as f:
		s = csv.reader(f)
		for row in s:
			to_generate.append(row[0])

	to_generate = list(set(to_generate) - set(do_not_generate))
	print(str(len(to_generate)) + ' items to generate.')

	manifest = [{'pmcid': x, 'doi': None, 'pmid': None} for x in to_generate]
	JournalArticles.item_creator(manifest)

if __name__ == '__main__':
	main('wikipedia_pmcid.csv')
