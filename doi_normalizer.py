import requests

lines_to_print = []

def process(wikidata_item, doi_list):
    canonical = doi_list[0].upper()
    canonical_is_present = False
    for doi in doi_list:
        if doi == canonical:
            canonical_is_present = True
        else:
            lines_to_print.append("-" + wikidata_item + "|P356|\"" + doi + "\"||")

    if canonical_is_present == False:
        lines_to_print.append(wikidata_item + "|P356|\"" + canonical + "\"||")

def main():
    # Canonical DOI format: all uppercase letters.
    # Three scenarios:
    #
    #   1. One DOI on an item that is identical with the canonical format:
    #      No action needed
    #
    #   2. One DOI on an item that is not identical with canonical format:
    #      Convert to canonical format
    #
    #   3. Two or more DOIs on a page that match with the canonical format when converted to uppercase:
    #      Check if canonically formatted DOI already on page and keep that. Remove non-matching ones.
    #      If none are in matching format, create an entry in the necessary format and delete the others.
    #
    #   4. Two or more DOIs that are different even when normalized to uppercase:
    #      We normalize within that set. Still duplicate, but at least normalized.

    url = "https://query.wikidata.org/sparql?format=json&query=select%20%3Fi%20%3Fdoi%20where%20%7B%20%3Fi%20wdt%3AP356%20%3Fdoi%20%7D%20order%20by%20%3Fi"
    seed = requests.get(url).json()["results"]["bindings"]

    manifest = {}  # dictionary of lists

    for result in seed:
        wikidata_item = result["i"]["value"].replace("http://www.wikidata.org/entity/", "")
        doi = result["doi"]["value"]

        if wikidata_item not in manifest:
            manifest[wikidata_item] = []

        manifest[wikidata_item].append(doi)

    for wikidata_item, doi_list in manifest.items():

        if len(doi_list) > 1:
            # Testing to see if all DOIs are the same when converted to uppercase.
            # If not, then it's case 4 and must be skipped.

            doi_variants = {}  # canonical form: variant
            for doi in doi_list:
                if doi.upper() not in doi_variants:
                    doi_variants[doi.upper()] = []
                doi_variants[doi.upper()].append(doi)
            for variant_list in doi_variants.values():
                process(wikidata_item, variant_list)

        else:
            if doi_list[0] != canonical:
                lines_to_print.append("-" + wikidata_item + "|P356|\"" + doi_list[0] + "\"||")
                lines_to_print.append(wikidata_item + "|P356|\"" + canonical + "\"||")

    packages = [lines_to_print[x:x+20000] for x in range(0, len(lines_to_print), 20000)]

    counter = 0
    for package in packages:
        with open('normalized-' + str(counter).zfill(2) + '.txt', 'w') as f:
            to_write = ''
            for line in package:
                to_write += line
            f.write(to_write)
        counter += 1


if __name__ == '__main__':
    main()
