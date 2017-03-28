import requests

def main():
    print("Initializing...")
    pmid_seed = "https://query.wikidata.org/sparql?format=json&query=SELECT%20%3Fi%20%3Fpm%20%3Fpmc%20%3Fc%20WHERE%20%7B%0A%20%20%3Fi%20wdt%3AP698%20%3Fpm%20.%0A%20%20OPTIONAL%20%7B%20%3Fi%20wdt%3AP932%20%3Fpmc%20%7D%0A%20%20OPTIONAL%20%7B%20%3Fi%20wdt%3AP2860%20%3Fc%20%7D%0A%7D"
    pmid_blob = requests.get(pmid_seed).json()

    pmid_to_wikidata = {}
    pmcid_to_wikidata = {}
    do_not_generate = {} # dictionary of lists
    pmid_list = []  # list of tuples: (wikidata item, PMID)

    for x in pmid_blob["results"]["bindings"]:
        item = x["i"]["value"].replace("http://www.wikidata.org/entity/", "")
        pmid = x["pm"]["value"]

        pmid_to_wikidata[pmid] = item

        if "pmc" not in x:  # Generation based on PMCID handled by other script
            pmid_list.append((item, pmid))

        if "pmc" in x:
            pmcid = x["pmc"]["value"]
            pmcid_to_wikidata[pmcid] = item

        if item not in do_not_generate:
            do_not_generate[item] = []

        if "c" in x:
            cites = x["c"]["value"].replace("http://www.wikidata.org/entity/", "")
            do_not_generate[item].append(cites)

    pmid_list = list(set(pmid_list))  # Removing duplicates
    pmid_list.sort()

    print("Processing " + str(len(pmid_list)) + " Wikidata entries")
    proc_counter = 1
    output_counter = 0

    OUTPUT = ""

    for pair in pmid_list:
        print("Processing entry " + str(proc_counter) + " – Number of output rows: " + str(output_counter), end="\r")
        proc_counter += 1
        wikidata_item = pair[0]
        pmid = pair[1]

        euroblob = requests.get("https://www.ebi.ac.uk/europepmc/webservices/rest/MED/{0}/references/1/1000/json".format(pmid)).json()

        if "referenceList" in euroblob:
            for reference in euroblob["referenceList"]["reference"]:
                if "source" in reference:
                    if "source" == "MED" and reference["id"] in pmid_to_wikidata:
                        cited_item = pmid_to_wikidata[reference["id"]]
                        if cited_item not in do_not_generate[wikidata_item]:
                            OUTPUT += wikidata_item + "\tP2860\t" + cited_item + "\n"
                            output_counter += 1
                    elif "source" == "PMC" and reference["id"].replace("PMC", "") in pmcid_to_wikidata:
                        cited_item = pmcid_to_wikidata[reference["id"].replace("PMC", "")]
                        if cited_item not in do_not_generate[wikidata_item]:
                            OUTPUT += wikidata_item + "\tP2860\t" + cited_item + "\n"
                            output_counter += 1

    print("")
    print(OUTPUT)

if __name__ == '__main__':
    main()