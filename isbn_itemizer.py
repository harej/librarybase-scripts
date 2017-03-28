import csv
import requests

seed = requests.get("https://query.wikidata.org/sparql?format=json&query=select%20%3Fitem%20%3Fisbn13%20where%20%7B%20%3Fitem%20wdt%3AP212%20%3Fisbn13%20%7D").json()["results"]["bindings"]
isbn13_to_wikidata = {x["isbn13"]["value"].replace("-", ""): x["item"]["value"].replace("http://www.wikidata.org/entity/", "") for x in seed}

seed = requests.get("https://query.wikidata.org/sparql?format=json&query=select%20%3Fitem%20%3Fisbn10%20where%20%7B%20%3Fitem%20wdt%3AP957%20%3Fisbn10%20%7D").json()["results"]["bindings"]
isbn10_to_wikidata = {x["isbn10"]["value"].replace("-", ""): x["item"]["value"].replace("http://www.wikidata.org/entity/", "") for x in seed}

with open("isbn_itemizer.csv") as f:
	spreadsheet = csv.reader(f)
	for row in spreadsheet:
		isbn = row[0].strip()
		title = row[1].strip()

		if len(isbn) == 13:
			if isbn not in isbn13_to_wikidata:
				print("CREATE")
				print("LAST\tP212\t\"" + isbn + "\"")
				print("LAST\tLen\t\"" + title + "\"")
				print("LAST\tDen\t\"book\"")
				print("LAST\tP1476\ten:\"" + title + "\"")
				print("LAST\tP31\tQ571")
		elif len(isbn) == 10:
			if isbn not in isbn10_to_wikidata:
				print("CREATE")
				print("LAST\tP957\t\"" + isbn + "\"")
				print("LAST\tLen\t\"" + title + "\"")
				print("LAST\tDen\t\"book\"")
				print("LAST\tP1476\ten:\"" + title + "\"")
				print("LAST\tP31\tQ571")