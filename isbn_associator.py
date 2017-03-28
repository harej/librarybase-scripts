import csv
import requests

seed = requests.get("https://query.wikidata.org/sparql?format=json&query=select%20%3Fi%20where%20%7B%0A%20%20%7B%0A%20%20%20%20%3Fi%20wdt%3AP2880%20%3Fn%20.%0A%20%20%20%20%3Fi%20wdt%3AP1433%20%3Fpublishedin%20.%0A%20%20%7D%20UNION%20%7B%0A%20%20%20%20%3Fi%20wdt%3AP2880%20%3Fn%20.%0A%20%20%20%20%3Fi%20wdt%3AP212%20%3Fisbn13%20.%0A%20%20%7D%20UNION%20%7B%0A%20%20%20%20%3Fi%20wdt%3AP2880%20%3Fn%20.%0A%20%20%20%20%3Fi%20wdt%3AP957%20%3Fisbn10%20.%0A%20%20%7D%0A%7D").json()["results"]["bindings"]
do_not_generate = [x["i"]["value"].replace("http://www.wikidata.org/entity/", "") for x in seed]

isbn_bank = {}  # maps ISBNs to their putative titles; to prevent duplicate lookup

with open("nioshtic_isbn.csv") as f:
    spreadsheet = csv.reader(f)
    for row in spreadsheet:
        wikidata_item = row[0]
        original_title = row[1]
        isbn = row[2]

        if wikidata_item in do_not_generate:
            continue

        if len(isbn) < 10:
            isbn = isbn.zfill(10)

        if isbn not in isbn_bank:
            get_title = requests.get("http://xissn.worldcat.org/webservices/xid/isbn/" + isbn + "?format=json&method=getMetadata&fl=title")
            if get_title.status_code != 200:
                continue
            try:
                blob = get_title.json()
            except:
                continue
            if "list" in blob:
                if len(blob["list"]) == 1:
                    if "title" in blob["list"][0]:
                        print(wikidata_item + "\t" + original_title + "\t" + isbn + "\t" + blob["list"][0]["title"])
                        isbn_bank[isbn] = blob["list"][0]["title"]
        else:
            print(wikidata_item + "\t" + original_title + "\t" + isbn + "\t" + isbn_bank[isbn])