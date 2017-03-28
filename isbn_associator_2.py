import csv
import requests

seed = requests.get("https://query.wikidata.org/sparql?format=json&query=select%20%3Fitem%20%3Fisbn13%20where%20%7B%20%3Fitem%20wdt%3AP212%20%3Fisbn13%20%7D").json()["results"]["bindings"]
isbn13_to_wikidata = {x["isbn13"]["value"].replace("-", ""): x["item"]["value"].replace("http://www.wikidata.org/entity/", "") for x in seed}

seed = requests.get("https://query.wikidata.org/sparql?format=json&query=select%20%3Fitem%20%3Fisbn10%20where%20%7B%20%3Fitem%20wdt%3AP957%20%3Fisbn10%20%7D").json()["results"]["bindings"]
isbn10_to_wikidata = {x["isbn10"]["value"].replace("-", ""): x["item"]["value"].replace("http://www.wikidata.org/entity/", "") for x in seed}

with open("isbn_associator.csv") as f:
	spreadsheet = csv.reader(f)
	for row in spreadsheet:
		item = row[0].strip()
		isbn = row[1].strip()  # item is published in isbn13

		if len(isbn) == 13:
			if isbn in isbn13_to_wikidata:
				print(item + "\tP1433\t" + isbn13_to_wikidata[isbn] + "\tS248\tQ26822184")
		elif len(isbn) == 10:
			if isbn in isbn10_to_wikidata:
				print(item + "\tP1433\t" + isbn10_to_wikidata[isbn] + "\tS248\tQ26822184")